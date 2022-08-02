package producer_consumer

import (
	"context"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/debug"
	eo_intr "sharedlog-stream/pkg/exactly_once_intr"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/txn_data"
	"sharedlog-stream/pkg/utils"
	"time"
)

type StreamSinkConfig[K, V any] struct {
	MsgSerde      commtypes.MessageSerdeG[K, V]
	FlushDuration time.Duration
}

type ShardedSharedLogStreamProducer[K, V any] struct {
	// syncutils.Mutex
	msgSerde      commtypes.MessageSerdeG[K, V]
	eom           eo_intr.ReadOnlyExactlyOnceManager
	stream        *sharedlog_stream.ShardedSharedLogStream
	name          string
	bufPush       bool
	guarantee     eo_intr.GuaranteeMth
	isFinalOutput bool
}

var _ = Producer(&ShardedSharedLogStreamProducer[int, string]{})

func NewShardedSharedLogStreamProducer[K, V any](stream *sharedlog_stream.ShardedSharedLogStream, config *StreamSinkConfig[K, V]) *ShardedSharedLogStreamProducer[K, V] {
	return &ShardedSharedLogStreamProducer[K, V]{
		msgSerde:      config.MsgSerde,
		stream:        stream,
		bufPush:       utils.CheckBufPush(),
		name:          "sink",
		isFinalOutput: false,
	}
}

func (sls *ShardedSharedLogStreamProducer[K, V]) MarkFinalOutput() {
	sls.isFinalOutput = true
}

func (sls *ShardedSharedLogStreamProducer[K, V]) IsFinalOutput() bool {
	return sls.isFinalOutput
}

func (sls *ShardedSharedLogStreamProducer[K, V]) ResetInitialProd() {
	sls.stream.ResetInitialProd()
}

func (sls *ShardedSharedLogStreamProducer[K, V]) GetInitialProdSeqNum(substreamNum uint8) uint64 {
	return sls.stream.GetInitialProdSeqNum(substreamNum)
}

func (sls *ShardedSharedLogStreamProducer[K, V]) GetCurrentProdSeqNum(substreamNum uint8) uint64 {
	return sls.stream.GetCurrentProdSeqNum(substreamNum)
}

// this method is not goroutine safe
func (sls *ShardedSharedLogStreamProducer[K, V]) SetName(name string) {
	sls.name = name
}

func (sls *ShardedSharedLogStreamProducer[K, V]) Name() string {
	return sls.name
}

func (sls *ShardedSharedLogStreamProducer[K, V]) Stream() sharedlog_stream.Stream {
	return sls.stream
}

func (sls *ShardedSharedLogStreamProducer[K, V]) ConfigExactlyOnce(
	eos eo_intr.ReadOnlyExactlyOnceManager,
	guarantee eo_intr.GuaranteeMth,
) {
	sls.guarantee = guarantee
	sls.stream.ExactlyOnce(guarantee)
	sls.eom = eos
}

func (sls *ShardedSharedLogStreamProducer[K, V]) Produce(ctx context.Context, msg commtypes.Message, parNum uint8, isControl bool) error {
	if utils.IsNil(msg.Key) && utils.IsNil(msg.Value) {
		return nil
	}
	if isControl {
		if sls.bufPush {
			err := sls.flush(ctx)
			if err != nil {
				return err
			}
		}
	}
	ctrl, ok := msg.Key.(string)
	if ok {
		if ctrl == txn_data.SCALE_FENCE_KEY {
			debug.Assert(isControl, "scale fence msg should be a control msg")
			scale_fence_tag := txn_data.ScaleFenceTag(sls.Stream().TopicNameHash(), parNum)
			nameTag := sharedlog_stream.NameHashWithPartition(sls.Stream().TopicNameHash(), parNum)
			_, err := sls.pushWithTag(ctx, msg.Value.([]byte), parNum, []uint64{scale_fence_tag, nameTag},
				nil, sharedlog_stream.StreamEntryMeta(isControl, false))
			return err
		}
		if ctrl == commtypes.END_OF_STREAM_KEY {
			debug.Assert(isControl, "end of stream should be a control msg")
			_, err := sls.push(ctx, msg.Value.([]byte), parNum,
				sharedlog_stream.StreamEntryMeta(isControl, false))
			// debug.Fprintf(os.Stderr, "Producer: push end of stream to %s %d at 0x%x\n", sls.Stream().TopicName(), parNum, off)
			return err
		}
	}
	bytes, err := sls.msgSerde.Encode(msg)
	if err != nil {
		return err
	}
	if bytes != nil {
		if sls.bufPush {
			if !isControl {
				return sls.bufpush(ctx, bytes, parNum)
			} else {
				err = sls.flush(ctx)
				if err != nil {
					return err
				}
				_, err = sls.push(ctx, bytes, parNum, sharedlog_stream.ControlRecordMeta)
				return err
			}
		} else {
			_, err = sls.push(ctx, bytes, parNum, sharedlog_stream.StreamEntryMeta(isControl, false))
			return err
		}
	}
	return nil
}

func (s *ShardedSharedLogStreamProducer[K, V]) TopicName() string { return s.stream.TopicName() }
func (s *ShardedSharedLogStreamProducer[K, V]) KeyEncoder() commtypes.Encoder {
	return commtypes.EncoderFunc(func(i interface{}) ([]byte, error) {
		return s.msgSerde.EncodeKey(i.(K))
	})
}
func (s *ShardedSharedLogStreamProducer[K, V]) Flush(ctx context.Context) error {
	if s.bufPush {
		err := s.flush(ctx)
		if err != nil {
			return err
		}
	}
	return nil
}
func (s *ShardedSharedLogStreamProducer[K, V]) FlushNoLock(ctx context.Context) error {
	if s.bufPush {
		err := FlushNoLock(ctx, s.stream, s.guarantee, s.eom.GetProducerId())
		if err != nil {
			return err
		}
	}
	return nil
}

func FlushNoLock(ctx context.Context, stream *sharedlog_stream.ShardedSharedLogStream, guarantee eo_intr.GuaranteeMth, producerId commtypes.ProducerId) error {
	if guarantee == eo_intr.TWO_PHASE_COMMIT || guarantee == eo_intr.EPOCH_MARK {
		return stream.FlushNoLock(ctx, producerId)
	} else {
		return stream.FlushNoLock(ctx, commtypes.EmptyProducerId)
	}
}

func (s *ShardedSharedLogStreamProducer[K, V]) flush(ctx context.Context) error {
	if s.guarantee == eo_intr.TWO_PHASE_COMMIT || s.guarantee == eo_intr.EPOCH_MARK {
		return s.stream.Flush(ctx, s.eom.GetProducerId())
	} else {
		return s.stream.Flush(ctx, commtypes.EmptyProducerId)
	}
}

func (s *ShardedSharedLogStreamProducer[K, V]) push(ctx context.Context, payload []byte, parNumber uint8, meta sharedlog_stream.LogEntryMeta) (uint64, error) {
	if s.guarantee == eo_intr.TWO_PHASE_COMMIT || s.guarantee == eo_intr.EPOCH_MARK {
		return s.stream.Push(ctx, payload, parNumber, meta, s.eom.GetProducerId())
	} else {
		return s.stream.Push(ctx, payload, parNumber, meta, commtypes.EmptyProducerId)
	}
}

func (s *ShardedSharedLogStreamProducer[K, V]) pushWithTag(ctx context.Context, payload []byte, parNumber uint8, tag []uint64,
	additionalTopic []string, meta sharedlog_stream.LogEntryMeta,
) (uint64, error) {
	if s.guarantee == eo_intr.TWO_PHASE_COMMIT || s.guarantee == eo_intr.EPOCH_MARK {
		return s.stream.PushWithTag(ctx, payload, parNumber, tag, additionalTopic, meta, s.eom.GetProducerId())
	} else {
		return s.stream.PushWithTag(ctx, payload, parNumber, tag, additionalTopic, meta, commtypes.EmptyProducerId)
	}
}

func (s *ShardedSharedLogStreamProducer[K, V]) bufpush(ctx context.Context, payload []byte, parNum uint8) error {
	if s.guarantee == eo_intr.TWO_PHASE_COMMIT || s.guarantee == eo_intr.EPOCH_MARK {
		return s.stream.BufPush(ctx, payload, parNum, s.eom.GetProducerId())
	} else {
		return s.stream.BufPush(ctx, payload, parNum, commtypes.EmptyProducerId)
	}
}
