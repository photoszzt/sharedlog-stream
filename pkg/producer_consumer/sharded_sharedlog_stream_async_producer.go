package producer_consumer

import (
	"context"
	"os"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/debug"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/exactly_once_intr"
	"sharedlog-stream/pkg/txn_data"
	"sharedlog-stream/pkg/utils"
	"sync"
	"time"
)

type ShardedSharedLogStreamAsyncProducer struct {
	wg            sync.WaitGroup
	kvmsgSerdes   commtypes.KVMsgSerdes
	tm            exactly_once_intr.ReadOnlyExactlyOnceManager
	streamPusher  *sharedlog_stream.StreamPush
	name          string
	flushDuration time.Duration
	bufPush       bool
	transactional bool
}

type StreamSinkConfig struct {
	KVMsgSerdes   commtypes.KVMsgSerdes
	FlushDuration time.Duration
}

func NewShardedSharedLogStreamAsyncSink(stream *sharedlog_stream.ShardedSharedLogStream, config *StreamSinkConfig) *ShardedSharedLogStreamAsyncProducer {
	streamPusher := sharedlog_stream.NewStreamPush(stream)
	debug.Assert(config.FlushDuration != 0, "flush duration cannot be zero")
	s := &ShardedSharedLogStreamAsyncProducer{
		kvmsgSerdes:   config.KVMsgSerdes,
		streamPusher:  streamPusher,
		flushDuration: config.FlushDuration,
		bufPush:       utils.CheckBufPush(),
		name:          "sink",
	}
	return s
}

func (sls *ShardedSharedLogStreamAsyncProducer) SetName(name string) {
	sls.name = name
}

func (sls *ShardedSharedLogStreamAsyncProducer) Name() string {
	return sls.name
}

func (sls *ShardedSharedLogStreamAsyncProducer) Stream() *sharedlog_stream.ShardedSharedLogStream {
	return sls.streamPusher.Stream
}

func (sls *ShardedSharedLogStreamAsyncProducer) ConfigExactlyOnce(tm exactly_once_intr.ReadOnlyExactlyOnceManager) {
	sls.transactional = true
	sls.tm = tm
}

func (sls *ShardedSharedLogStreamAsyncProducer) StartAsyncPushWithTick(ctx context.Context) {
	sls.wg.Add(1)
	if sls.transactional {
		producerId := sls.tm.GetProducerId()
		go sls.streamPusher.AsyncStreamPush(ctx, &sls.wg, producerId)
	} else {
		go sls.streamPusher.AsyncStreamPush(ctx, &sls.wg, sharedlog_stream.EmptyProducerId)
	}
}

func (sls *ShardedSharedLogStreamAsyncProducer) StartAsyncPushNoTick(ctx context.Context) {
	sls.wg.Add(1)
	if sls.transactional {
		producerId := sls.tm.GetProducerId()
		go sls.streamPusher.AsyncStreamPushNoTick(ctx, &sls.wg, producerId)
	} else {
		go sls.streamPusher.AsyncStreamPushNoTick(ctx, &sls.wg, sharedlog_stream.EmptyProducerId)
	}
}

func (sls *ShardedSharedLogStreamAsyncProducer) RebuildMsgChan() {
	sls.streamPusher.MsgChan = make(chan sharedlog_stream.PayloadToPush, sharedlog_stream.MSG_CHAN_SIZE)
}

func (sls *ShardedSharedLogStreamAsyncProducer) CloseAsyncPush() {
	debug.Fprintf(os.Stderr, "stream pusher msg chan len: %d\n", len(sls.streamPusher.MsgChan))
	// for len(sls.streamPusher.MsgChan) > 0 {
	// 	debug.Fprintf(os.Stderr, "stream pusher msg chan len: %d\n", len(sls.streamPusher.MsgChan))
	// 	time.Sleep(time.Duration(1) * time.Millisecond)
	// }
	close(sls.streamPusher.MsgChan)
	sls.wg.Wait()
}

func (sls *ShardedSharedLogStreamAsyncProducer) TopicName() string {
	return sls.streamPusher.Stream.TopicName()
}

func MsgIsScaleFence(msg *commtypes.Message) bool {
	ctrl, ok := msg.Key.(string)
	return ok && ctrl == txn_data.SCALE_FENCE_KEY
}

func (sls *ShardedSharedLogStreamAsyncProducer) Produce(ctx context.Context, msg commtypes.Message, parNum uint8, isControl bool) error {
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

func (sls *ShardedSharedLogStreamAsyncProducer) KeySerde() commtypes.Serde {
	return sls.kvmsgSerdes.KeySerde
}

func (sls *ShardedSharedLogStreamAsyncProducer) Flush(ctx context.Context) error {
	if sls.transactional {
		producerId := sls.tm.GetProducerId()
		return sls.streamPusher.Flush(ctx, producerId)
	} else {
		return sls.streamPusher.Flush(ctx, sharedlog_stream.EmptyProducerId)
	}
}

func (sls *ShardedSharedLogStreamAsyncProducer) FlushNoLock(ctx context.Context) error {
	if sls.transactional {
		producerId := sls.tm.GetProducerId()
		return sls.streamPusher.FlushNoLock(ctx, producerId)
	} else {
		return sls.streamPusher.Flush(ctx, sharedlog_stream.EmptyProducerId)
	}
}

func (sls *ShardedSharedLogStreamAsyncProducer) InitFlushTimer() {
	sls.streamPusher.InitFlushTimer(sls.flushDuration)
}
