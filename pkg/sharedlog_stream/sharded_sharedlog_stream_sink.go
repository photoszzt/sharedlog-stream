package sharedlog_stream

import (
	"context"
	"os"
	"sharedlog-stream/pkg/debug"
	"sharedlog-stream/pkg/stream/processor"
	"sharedlog-stream/pkg/stream/processor/commtypes"
)

type ShardedSharedLogStreamSink struct {
	keySerde   commtypes.Serde
	valueSerde commtypes.Serde
	msgSerde   commtypes.MsgSerde
	stream     *ShardedSharedLogStream

	bufPush bool
}

type StreamSinkConfig struct {
	KeySerde     commtypes.Serde
	ValueSerde   commtypes.Serde
	MsgSerde     commtypes.MsgSerde
	streamPusher *StreamPush
}

var _ = processor.Sink(&ShardedSharedLogStreamSink{})

func NewShardedSharedLogStreamSink(stream *ShardedSharedLogStream, config *StreamSinkConfig) *ShardedSharedLogStreamSink {
	bufPush_str := os.Getenv("BUFPUSH")
	bufPush := false
	if bufPush_str == "true" || bufPush_str == "1" {
		bufPush = true
	}
	return &ShardedSharedLogStreamSink{
		stream:     stream,
		keySerde:   config.KeySerde,
		valueSerde: config.ValueSerde,
		msgSerde:   config.MsgSerde,
		bufPush:    bufPush,
	}
}

func (sls *ShardedSharedLogStreamSink) TopicName() string {
	return sls.stream.TopicName()
}

func (sls *ShardedSharedLogStreamSink) Sink(ctx context.Context, msg commtypes.Message, parNum uint8, isControl bool) error {
	if msg.Key == nil && msg.Value == nil {
		return nil
	}
	ctrl, ok := msg.Key.(string)
	if ok && ctrl == commtypes.SCALE_FENCE_KEY {
		debug.Assert(isControl, "scale fence msg should be a control msg")
		if sls.bufPush && isControl {
			err := sls.stream.Flush(ctx)
			if err != nil {
				return err
			}
		}
		_, err := sls.stream.Push(ctx, msg.Value.([]byte), parNum, isControl, false)
		return err
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
		if sls.bufPush {
			if !isControl {
				return sls.stream.BufPush(ctx, bytes, parNum)
			}
			err = sls.stream.Flush(ctx)
			if err != nil {
				return err
			}
			_, err = sls.stream.Push(ctx, bytes, parNum, isControl, false)
			if err != nil {
				return err
			}
		} else {
			_, err = sls.stream.Push(ctx, bytes, parNum, isControl, false)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (sls *ShardedSharedLogStreamSink) KeySerde() commtypes.Serde {
	return sls.keySerde
}

func (sls *ShardedSharedLogStreamSink) Flush(ctx context.Context) error {
	if sls.bufPush {
		return sls.stream.Flush(ctx)
	}
	return nil
}
