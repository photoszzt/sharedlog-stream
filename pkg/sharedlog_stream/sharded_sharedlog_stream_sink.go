package sharedlog_stream

import (
	"context"
	"sharedlog-stream/pkg/debug"
	"sharedlog-stream/pkg/stream/processor"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"sync"
	"time"
)

type ShardedSharedLogStreamSink struct {
	keySerde      commtypes.Serde
	valueSerde    commtypes.Serde
	msgSerde      commtypes.MsgSerde
	wg            sync.WaitGroup
	streamPusher  *StreamPush
	flushDuration time.Duration
}

type StreamSinkConfig struct {
	KeySerde      commtypes.Serde
	ValueSerde    commtypes.Serde
	MsgSerde      commtypes.MsgSerde
	FlushDuration time.Duration
}

var _ = processor.Sink(&ShardedSharedLogStreamSink{})

func NewShardedSharedLogStreamSink(stream *ShardedSharedLogStream, config *StreamSinkConfig) *ShardedSharedLogStreamSink {
	/*
		bufPush_str := os.Getenv("BUFPUSH")
		bufPush := false
		if bufPush_str == "true" || bufPush_str == "1" {
			bufPush = true
		}
	*/
	streamPusher := NewStreamPush(stream)
	debug.Assert(config.FlushDuration != 0, "flush duration cannot be zero")
	s := &ShardedSharedLogStreamSink{
		keySerde:      config.KeySerde,
		valueSerde:    config.ValueSerde,
		msgSerde:      config.MsgSerde,
		streamPusher:  streamPusher,
		flushDuration: config.FlushDuration,
	}
	return s
}

func (sls *ShardedSharedLogStreamSink) StartAsyncPushWithTick(ctx context.Context) {
	sls.wg.Add(1)
	go sls.streamPusher.AsyncStreamPush(ctx, &sls.wg)
}

func (sls *ShardedSharedLogStreamSink) StartAsyncPushNoTick(ctx context.Context) {
	sls.wg.Add(1)
	go sls.streamPusher.AsyncStreamPushNoTick(ctx, &sls.wg)
}

func (sls *ShardedSharedLogStreamSink) CloseAsyncPush() {
	close(sls.streamPusher.MsgChan)
	if sls.streamPusher.BufPush {
		if sls.streamPusher.FlushTimer != nil {
			sls.streamPusher.FlushTimer.Stop()
		}
	}
	sls.wg.Wait()
}

func (sls *ShardedSharedLogStreamSink) TopicName() string {
	return sls.streamPusher.Stream.TopicName()
}

func (sls *ShardedSharedLogStreamSink) Sink(ctx context.Context, msg commtypes.Message, parNum uint8, isControl bool) error {
	if msg.Key == nil && msg.Value == nil {
		return nil
	}
	ctrl, ok := msg.Key.(string)
	if ok && ctrl == commtypes.SCALE_FENCE_KEY {
		debug.Assert(isControl, "scale fence msg should be a control msg")
		/*
			if sls.bufPush && isControl {
				err := sls.stream.Flush(ctx)
				if err != nil {
					return err
				}
			}
			_, err := sls.stream.Push(ctx, msg.Value.([]byte), parNum, isControl, false)
			return err
		*/
		sls.streamPusher.MsgChan <- PayloadToPush{Payload: msg.Value.([]byte),
			Partitions: []uint8{parNum}, IsControl: isControl}
		return nil
	}
	var keyEncoded []byte
	if msg.Key != nil {
		// debug.Fprintf(os.Stderr, "sls: %v, key encoder: %v\n", sls, sls.keySerde)
		keyEncodedTmp, err := sls.keySerde.Encode(msg.Key)
		if err != nil {
			return err
		}
		keyEncoded = keyEncodedTmp
	}
	valEncoded, err := sls.valueSerde.Encode(msg.Value)
	if err != nil {
		return err
	}
	bytes, err := sls.msgSerde.Encode(keyEncoded, valEncoded)
	if err != nil {
		return err
	}
	if bytes != nil {
		sls.streamPusher.MsgChan <- PayloadToPush{Payload: bytes,
			Partitions: []uint8{parNum}, IsControl: isControl}
		/*
			if sls.bufPush {
				if !isControl {
					return sls.stream.BufPush(ctx, bytes, parNum)
				} else {
					err = sls.stream.Flush(ctx)
					if err != nil {
						return err
					}
					_, err = sls.stream.Push(ctx, bytes, parNum, isControl, false)
					if err != nil {
						return err
					}
				}
			} else {
				_, err = sls.stream.Push(ctx, bytes, parNum, isControl, false)
				if err != nil {
					return err
				}
			}
		*/
	}
	return nil
}

func (sls *ShardedSharedLogStreamSink) KeySerde() commtypes.Serde {
	return sls.keySerde
}

func (sls *ShardedSharedLogStreamSink) Flush(ctx context.Context) error {
	return sls.streamPusher.Flush(ctx)
}

func (sls *ShardedSharedLogStreamSink) FlushNoLock(ctx context.Context) error {
	return sls.streamPusher.FlushNoLock(ctx)
}

func (sls *ShardedSharedLogStreamSink) InitFlushTimer() {
	sls.streamPusher.InitFlushTimer(sls.flushDuration)
}
