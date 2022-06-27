package producer_consumer

import (
	"context"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/debug"
	eo_intr "sharedlog-stream/pkg/exactly_once_intr"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/txn_data"
	"sharedlog-stream/pkg/utils"
	"sync"
	"time"
)

type StreamSinkConfig struct {
	MsgSerde      commtypes.MessageSerde
	FlushDuration time.Duration
}

type ShardedSharedLogStreamProducer struct {
	sync.Mutex
	msgSerde  commtypes.MessageSerde
	eom       eo_intr.ReadOnlyExactlyOnceManager
	stream    *sharedlog_stream.ShardedSharedLogStream
	name      string
	bufPush   bool
	guarantee eo_intr.GuaranteeMth
}

var _ = Producer(&ShardedSharedLogStreamProducer{})

func NewShardedSharedLogStreamProducer(stream *sharedlog_stream.ShardedSharedLogStream, config *StreamSinkConfig) *ShardedSharedLogStreamProducer {
	return &ShardedSharedLogStreamProducer{
		msgSerde: config.MsgSerde,
		stream:   stream,
		bufPush:  utils.CheckBufPush(),
		name:     "sink",
	}
}

func (sls *ShardedSharedLogStreamProducer) ResetInitialProd() {
	sls.stream.ResetInitialProd()
}

func (sls *ShardedSharedLogStreamProducer) GetInitialProdSeqNum(substreamNum uint8) uint64 {
	return sls.stream.GetInitialProdSeqNum(substreamNum)
}

func (sls *ShardedSharedLogStreamProducer) GetCurrentProdSeqNum(substreamNum uint8) uint64 {
	return sls.stream.GetCurrentProdSeqNum(substreamNum)
}

// this method is not goroutine safe
func (sls *ShardedSharedLogStreamProducer) SetName(name string) {
	sls.name = name
}

func (sls *ShardedSharedLogStreamProducer) Name() string {
	return sls.name
}

func (sls *ShardedSharedLogStreamProducer) Stream() sharedlog_stream.Stream {
	return sls.stream
}

func (sls *ShardedSharedLogStreamProducer) ConfigExactlyOnce(
	eos eo_intr.ReadOnlyExactlyOnceManager,
	guarantee eo_intr.GuaranteeMth,
) {
	sls.guarantee = guarantee
	sls.stream.ExactlyOnce(guarantee)
	sls.eom = eos
}

func (sls *ShardedSharedLogStreamProducer) Produce(ctx context.Context, msg commtypes.Message, parNum uint8, isControl bool) error {
	if msg.Key == nil && msg.Value == nil {
		return nil
	}
	ctrl, ok := msg.Key.(string)
	if ok && ctrl == txn_data.SCALE_FENCE_KEY {
		debug.Assert(isControl, "scale fence msg should be a control msg")
		if sls.bufPush && isControl {
			err := sls.flush(ctx)
			if err != nil {
				return err
			}
		}
		scale_fence_tag := txn_data.ScaleFenceTag(sls.Stream().TopicNameHash(), parNum)
		_, err := sls.pushWithTag(ctx, msg.Value.([]byte), parNum, []uint64{scale_fence_tag},
			nil, sharedlog_stream.StreamEntryMeta(isControl, false))
		return err
	}
	bytes, err := sls.msgSerde.Encode(&msg)
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

func (s *ShardedSharedLogStreamProducer) TopicName() string { return s.stream.TopicName() }
func (s *ShardedSharedLogStreamProducer) KeySerde() commtypes.Serde {
	return s.msgSerde.GetKeySerde()
}
func (s *ShardedSharedLogStreamProducer) Flush(ctx context.Context) error {
	if s.bufPush {
		err := s.flush(ctx)
		if err != nil {
			return err
		}
	}
	return nil
}
func (s *ShardedSharedLogStreamProducer) FlushNoLock(ctx context.Context) error {
	if s.bufPush {
		err := s.flushNoLock(ctx)
		if err != nil {
			return err
		}
	}
	return nil
}
func (s *ShardedSharedLogStreamProducer) InitFlushTimer() {}

func (s *ShardedSharedLogStreamProducer) flushNoLock(ctx context.Context) error {
	if s.guarantee == eo_intr.TWO_PHASE_COMMIT || s.guarantee == eo_intr.EPOCH_MARK {
		producerId := s.eom.GetProducerId()
		return s.stream.FlushNoLock(ctx, producerId)
	} else {
		return s.stream.FlushNoLock(ctx, commtypes.EmptyProducerId)
	}
}

func (s *ShardedSharedLogStreamProducer) flush(ctx context.Context) error {
	if s.guarantee == eo_intr.TWO_PHASE_COMMIT || s.guarantee == eo_intr.EPOCH_MARK {
		producerId := s.eom.GetProducerId()
		return s.stream.Flush(ctx, producerId)
	} else {
		return s.stream.Flush(ctx, commtypes.EmptyProducerId)
	}
}

func (s *ShardedSharedLogStreamProducer) push(ctx context.Context, payload []byte, parNumber uint8, meta sharedlog_stream.LogEntryMeta) (uint64, error) {
	if s.guarantee == eo_intr.TWO_PHASE_COMMIT || s.guarantee == eo_intr.EPOCH_MARK {
		producerId := s.eom.GetProducerId()
		return s.stream.Push(ctx, payload, parNumber, meta, producerId)
	} else {
		return s.stream.Push(ctx, payload, parNumber, meta, commtypes.EmptyProducerId)
	}
}

func (s *ShardedSharedLogStreamProducer) pushWithTag(ctx context.Context, payload []byte, parNumber uint8, tag []uint64,
	additionalTopic []string, meta sharedlog_stream.LogEntryMeta,
) (uint64, error) {
	if s.guarantee == eo_intr.TWO_PHASE_COMMIT || s.guarantee == eo_intr.EPOCH_MARK {
		return s.stream.PushWithTag(ctx, payload, parNumber, tag, additionalTopic, meta, s.eom.GetProducerId())
	} else {
		return s.stream.PushWithTag(ctx, payload, parNumber, tag, additionalTopic, meta, commtypes.EmptyProducerId)
	}
}

func (s *ShardedSharedLogStreamProducer) bufpush(ctx context.Context, payload []byte, parNum uint8) error {
	if s.guarantee == eo_intr.TWO_PHASE_COMMIT {
		return s.stream.BufPush(ctx, payload, parNum, s.eom.GetProducerId())
	} else {
		return s.stream.BufPush(ctx, payload, parNum, commtypes.EmptyProducerId)
	}
}
