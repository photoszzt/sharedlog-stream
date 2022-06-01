package source_sink

import (
	"context"
	"os"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/debug"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/transaction/tran_interface"
	"sharedlog-stream/pkg/txn_data"
	"sharedlog-stream/pkg/utils"
	"sync"
	"time"
)

type ShardedSharedLogStreamSink struct {
	wg            sync.WaitGroup
	kvmsgSerdes   commtypes.KVMsgSerdes
	tm            tran_interface.ReadOnlyTransactionManager
	streamPusher  *sharedlog_stream.StreamPush
	flushDuration time.Duration
	bufPush       bool
	transactional bool
}

type StreamSinkConfig struct {
	KVMsgSerdes   commtypes.KVMsgSerdes
	FlushDuration time.Duration
}

var _ = Sink(&ShardedSharedLogStreamSink{})

func NewShardedSharedLogStreamSink(stream *sharedlog_stream.ShardedSharedLogStream, config *StreamSinkConfig) *ShardedSharedLogStreamSink {
	streamPusher := sharedlog_stream.NewStreamPush(stream)
	debug.Assert(config.FlushDuration != 0, "flush duration cannot be zero")
	s := &ShardedSharedLogStreamSink{
		kvmsgSerdes:   config.KVMsgSerdes,
		streamPusher:  streamPusher,
		flushDuration: config.FlushDuration,
		bufPush:       utils.CheckBufPush(),
	}
	return s
}

func (sls *ShardedSharedLogStreamSink) Stream() *sharedlog_stream.ShardedSharedLogStream {
	return sls.streamPusher.Stream
}

func (sls *ShardedSharedLogStreamSink) InTransaction(tm tran_interface.ReadOnlyTransactionManager) {
	sls.transactional = true
	sls.tm = tm
}

func (sls *ShardedSharedLogStreamSink) StartAsyncPushWithTick(ctx context.Context) {
	sls.wg.Add(1)
	if sls.transactional {
		go sls.streamPusher.AsyncStreamPush(ctx, &sls.wg, sls.tm.GetCurrentTaskId(), sls.tm.GetCurrentEpoch(), sls.tm.GetTransactionID())
	} else {
		go sls.streamPusher.AsyncStreamPush(ctx, &sls.wg, 0, 0, 0)
	}
}

func (sls *ShardedSharedLogStreamSink) StartAsyncPushNoTick(ctx context.Context) {
	sls.wg.Add(1)
	if sls.transactional {
		go sls.streamPusher.AsyncStreamPushNoTick(ctx, &sls.wg, sls.tm.GetCurrentTaskId(), sls.tm.GetCurrentEpoch(), sls.tm.GetTransactionID())
	} else {
		go sls.streamPusher.AsyncStreamPushNoTick(ctx, &sls.wg, 0, 0, 0)
	}
}

func (sls *ShardedSharedLogStreamSink) RebuildMsgChan() {
	sls.streamPusher.MsgChan = make(chan sharedlog_stream.PayloadToPush, sharedlog_stream.MSG_CHAN_SIZE)
}

func (sls *ShardedSharedLogStreamSink) CloseAsyncPush() {
	debug.Fprintf(os.Stderr, "stream pusher msg chan len: %d\n", len(sls.streamPusher.MsgChan))
	// for len(sls.streamPusher.MsgChan) > 0 {
	// 	debug.Fprintf(os.Stderr, "stream pusher msg chan len: %d\n", len(sls.streamPusher.MsgChan))
	// 	time.Sleep(time.Duration(1) * time.Millisecond)
	// }
	close(sls.streamPusher.MsgChan)
	sls.wg.Wait()
}

func (sls *ShardedSharedLogStreamSink) TopicName() string {
	return sls.streamPusher.Stream.TopicName()
}

func MsgIsScaleFence(msg *commtypes.Message) bool {
	ctrl, ok := msg.Key.(string)
	return ok && ctrl == txn_data.SCALE_FENCE_KEY
}

func (sls *ShardedSharedLogStreamSink) Produce(ctx context.Context, msg commtypes.Message, parNum uint8, isControl bool) error {
	if msg.Key == nil && msg.Value == nil {
		return nil
	}
	if MsgIsScaleFence(&msg) {
		debug.Assert(isControl, "scale fence msg should be a control msg")
		sls.streamPusher.MsgChan <- sharedlog_stream.PayloadToPush{Payload: msg.Value.([]byte),
			Partitions: []uint8{parNum}, IsControl: isControl}
		return nil
	}
	bytes, err := commtypes.EncodeMsg(msg, sls.kvmsgSerdes)
	if err != nil {
		return err
	}
	if bytes != nil {
		sls.streamPusher.MsgChan <- sharedlog_stream.PayloadToPush{Payload: bytes,
			Partitions: []uint8{parNum}, IsControl: isControl}
	}
	return nil
}

func (sls *ShardedSharedLogStreamSink) KeySerde() commtypes.Serde {
	return sls.kvmsgSerdes.KeySerde
}

func (sls *ShardedSharedLogStreamSink) Flush(ctx context.Context) error {
	if sls.transactional {
		return sls.streamPusher.Flush(ctx, sls.tm.GetCurrentTaskId(), sls.tm.GetCurrentEpoch(), sls.tm.GetTransactionID())
	} else {
		return sls.streamPusher.Flush(ctx, 0, 0, 0)
	}
}

func (sls *ShardedSharedLogStreamSink) FlushNoLock(ctx context.Context) error {
	if sls.transactional {
		return sls.streamPusher.FlushNoLock(ctx, sls.tm.GetCurrentTaskId(), sls.tm.GetCurrentEpoch(), sls.tm.GetTransactionID())
	} else {
		return sls.streamPusher.Flush(ctx, 0, 0, 0)
	}
}

func (sls *ShardedSharedLogStreamSink) InitFlushTimer() {
	sls.streamPusher.InitFlushTimer(sls.flushDuration)
}
