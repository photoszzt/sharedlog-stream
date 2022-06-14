package source_sink

import (
	"context"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/debug"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/transaction/tran_interface"
	"sharedlog-stream/pkg/txn_data"
	"sharedlog-stream/pkg/utils"
)

type ShardedSharedLogStreamSyncSink struct {
	kvmsgSerdes   commtypes.KVMsgSerdes
	tm            tran_interface.ReadOnlyExactlyOnceManager
	stream        *sharedlog_stream.ShardedSharedLogStream
	name          string
	bufPush       bool
	transactional bool
}

var _ = Sink(&ShardedSharedLogStreamSyncSink{})

func NewShardedSharedLogStreamSyncSink(stream *sharedlog_stream.ShardedSharedLogStream, config *StreamSinkConfig) *ShardedSharedLogStreamSyncSink {
	return &ShardedSharedLogStreamSyncSink{
		kvmsgSerdes: config.KVMsgSerdes,
		stream:      stream,
		bufPush:     utils.CheckBufPush(),
		name:        "sink",
	}
}

// this method is not goroutine safe
func (sls *ShardedSharedLogStreamSyncSink) SetName(name string) {
	sls.name = name
}

func (sls *ShardedSharedLogStreamSyncSink) Name() string {
	return sls.name
}

func (sls *ShardedSharedLogStreamSyncSink) Stream() *sharedlog_stream.ShardedSharedLogStream {
	return sls.stream
}

func (sls *ShardedSharedLogStreamSyncSink) InTransaction(tm tran_interface.ReadOnlyExactlyOnceManager) {
	sls.transactional = true
	sls.tm = tm
}

func (sls *ShardedSharedLogStreamSyncSink) Produce(ctx context.Context, msg commtypes.Message, parNum uint8, isControl bool) error {
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
		_, err := sls.pushWithTag(ctx, msg.Value.([]byte), parNum, []uint64{scale_fence_tag}, isControl, false)
		return err
	}
	bytes, err := commtypes.EncodeMsg(msg, sls.kvmsgSerdes)
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
				_, err = sls.push(ctx, bytes, parNum, isControl, false)
				if err != nil {
					return err
				}
			}
		} else {
			_, err = sls.push(ctx, bytes, parNum, isControl, false)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
func (s *ShardedSharedLogStreamSyncSink) TopicName() string { return s.stream.TopicName() }
func (s *ShardedSharedLogStreamSyncSink) KeySerde() commtypes.Serde {
	return s.kvmsgSerdes.KeySerde
}
func (s *ShardedSharedLogStreamSyncSink) Flush(ctx context.Context) error {
	if s.bufPush {
		err := s.flush(ctx)
		if err != nil {
			return err
		}
	}
	return nil
}
func (s *ShardedSharedLogStreamSyncSink) FlushNoLock(ctx context.Context) error {
	if s.bufPush {
		err := s.flushNoLock(ctx)
		if err != nil {
			return err
		}
	}
	return nil
}
func (s *ShardedSharedLogStreamSyncSink) InitFlushTimer() {}

func (s *ShardedSharedLogStreamSyncSink) flushNoLock(ctx context.Context) error {
	if s.transactional {
		return s.stream.FlushNoLock(ctx,
			s.tm.GetCurrentTaskId(), s.tm.GetCurrentEpoch(), s.tm.GetTransactionID())
	} else {
		return s.stream.FlushNoLock(ctx, 0, 0, 0)
	}
}

func (s *ShardedSharedLogStreamSyncSink) flush(ctx context.Context) error {
	if s.transactional {
		return s.stream.Flush(ctx, s.tm.GetCurrentTaskId(), s.tm.GetCurrentEpoch(), s.tm.GetTransactionID())
	} else {
		return s.stream.Flush(ctx, 0, 0, 0)
	}
}

func (s *ShardedSharedLogStreamSyncSink) push(ctx context.Context, payload []byte, parNumber uint8, isControl bool, payloadIsArr bool) (uint64, error) {
	if s.transactional {
		return s.stream.Push(ctx, payload, parNumber, isControl, payloadIsArr, s.tm.GetCurrentTaskId(), s.tm.GetCurrentEpoch(), s.tm.GetTransactionID())
	} else {
		return s.stream.Push(ctx, payload, parNumber, isControl, payloadIsArr, 0, 0, 0)
	}
}

func (s *ShardedSharedLogStreamSyncSink) pushWithTag(ctx context.Context, payload []byte, parNumber uint8, tag []uint64, isControl bool, payloadIsArr bool) (uint64, error) {
	if s.transactional {
		return s.stream.PushWithTag(ctx, payload, parNumber, tag, isControl, payloadIsArr, s.tm.GetCurrentTaskId(), s.tm.GetCurrentEpoch(), s.tm.GetTransactionID())
	} else {
		return s.stream.PushWithTag(ctx, payload, parNumber, tag, isControl, payloadIsArr, 0, 0, 0)
	}
}

func (s *ShardedSharedLogStreamSyncSink) bufpush(ctx context.Context, payload []byte, parNum uint8) error {
	if s.transactional {
		return s.stream.BufPush(ctx, payload, parNum, s.tm.GetCurrentTaskId(), s.tm.GetCurrentEpoch(), s.tm.GetCurrentTaskId())
	} else {
		return s.stream.BufPush(ctx, payload, parNum, 0, 0, 0)
	}
}
