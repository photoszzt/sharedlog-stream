package producer_consumer

import (
	"context"
	"fmt"
	"sharedlog-stream/pkg/commtypes"
	eo_intr "sharedlog-stream/pkg/exactly_once_intr"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/txn_data"
	"sharedlog-stream/pkg/utils"
	"time"
)

type StreamSinkConfig struct {
	// MsgSerde      commtypes.MessageSerdeG
	FlushDuration time.Duration
	Format        commtypes.SerdeFormat
}

type ShardedSharedLogStreamProducer struct {
	// syncutils.Mutex
	// msgSerde      commtypes.MessageSerdeG
	msgSerSerde   commtypes.SerdeG[commtypes.MessageSerialized]
	eom           eo_intr.ReadOnlyExactlyOnceManager
	stream        *sharedlog_stream.ShardedSharedLogStream
	name          string
	bufPush       bool
	guarantee     eo_intr.GuaranteeMth
	isFinalOutput bool
}

var _ = Producer(&ShardedSharedLogStreamProducer{})

func NewShardedSharedLogStreamProducer(stream *sharedlog_stream.ShardedSharedLogStream,
	config *StreamSinkConfig,
) *ShardedSharedLogStreamProducer {
	msgSerSerde := commtypes.GetMessageSerializedSerdeG(config.Format)
	return &ShardedSharedLogStreamProducer{
		// msgSerde:      config.MsgSerde,
		msgSerSerde:   msgSerSerde,
		stream:        stream,
		bufPush:       utils.CheckBufPush(),
		name:          "sink",
		isFinalOutput: false,
	}
}

func (sls *ShardedSharedLogStreamProducer) MarkFinalOutput() {
	sls.isFinalOutput = true
}

func (sls *ShardedSharedLogStreamProducer) IsFinalOutput() bool {
	return sls.isFinalOutput
}

func (sls *ShardedSharedLogStreamProducer) ResetInitialProd() {
	sls.stream.ResetInitialProd()
}

func (sls *ShardedSharedLogStreamProducer) GetInitialProdSeqNum(substreamNum uint8) uint64 {
	return sls.stream.GetInitialProdSeqNum(substreamNum)
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

func (sls *ShardedSharedLogStreamProducer) ProduceCtrlMsg(ctx context.Context, msg commtypes.RawMsgAndSeq, parNums []uint8) (int, error) {
	if sls.bufPush {
		err := sls.flush(ctx)
		if err != nil {
			return 0, err
		}
	}
	if msg.Mark == commtypes.SCALE_FENCE {
		for _, parNum := range parNums {
			scale_fence_tag := txn_data.ScaleFenceTag(sls.Stream().TopicNameHash(), parNum)
			nameTag := sharedlog_stream.NameHashWithPartition(sls.Stream().TopicNameHash(), parNum)
			_, err := sls.pushWithTag(ctx, msg.Payload, parNum, []uint64{scale_fence_tag, nameTag},
				nil, sharedlog_stream.ControlRecordMeta)
			if err != nil {
				return 0, err
			}
			// fmt.Fprintf(os.Stderr, "produce scale fence %s(%d): offset 0x%x\n", sls.TopicName(), parNum, off)
		}
		return len(parNums), nil
	} else if msg.Mark == commtypes.STREAM_END {
		for _, parNum := range parNums {
			_, err := sls.push(ctx, msg.Payload, parNum, sharedlog_stream.ControlRecordMeta)
			// debug.Fprintf(os.Stderr, "Producer: push end of stream to %s %d at 0x%x\n", sls.Stream().TopicName(), parNum, off)
			if err != nil {
				return 0, err
			}
		}
		return len(parNums), nil
	} else {
		return 0, fmt.Errorf("unknown control message with mark: %s", msg.Mark)
	}
}

func (sls *ShardedSharedLogStreamProducer) ProduceData(ctx context.Context, msgSer commtypes.MessageSerialized, parNum uint8) error {
	bytes, err := sls.msgSerSerde.Encode(msgSer)
	if err != nil {
		return err
	}
	if bytes != nil {
		if sls.bufPush {
			return sls.bufpush(ctx, bytes, parNum)
		} else {
			_, err := sls.push(ctx, bytes, parNum, sharedlog_stream.StreamEntryMeta(false, false))
			return err
		}
	}
	return nil
}

func (s *ShardedSharedLogStreamProducer) TopicName() string { return s.stream.TopicName() }
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

func (s *ShardedSharedLogStreamProducer) flush(ctx context.Context) error {
	if s.guarantee == eo_intr.TWO_PHASE_COMMIT || s.guarantee == eo_intr.EPOCH_MARK {
		return s.stream.Flush(ctx, s.eom.GetProducerId())
	} else {
		return s.stream.Flush(ctx, commtypes.EmptyProducerId)
	}
}

func (s *ShardedSharedLogStreamProducer) push(ctx context.Context, payload []byte, parNumber uint8, meta sharedlog_stream.LogEntryMeta) (uint64, error) {
	if s.guarantee == eo_intr.TWO_PHASE_COMMIT || s.guarantee == eo_intr.EPOCH_MARK {
		return s.stream.Push(ctx, payload, parNumber, meta, s.eom.GetProducerId())
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
	if s.guarantee == eo_intr.TWO_PHASE_COMMIT || s.guarantee == eo_intr.EPOCH_MARK {
		return s.stream.BufPush(ctx, payload, parNum, s.eom.GetProducerId())
	} else {
		return s.stream.BufPush(ctx, payload, parNum, commtypes.EmptyProducerId)
	}
}
