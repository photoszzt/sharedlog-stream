package sharedlog_stream

import (
	"context"
	"sharedlog-stream/pkg/stream/processor"
	"sharedlog-stream/pkg/stream/processor/commtypes"
)

type ShardedSharedLogStreamSink struct {
	// pipe         processor.Pipe
	// pctx         processor.ProcessorContext
	stream     *ShardedSharedLogStream
	keySerde   commtypes.Serde
	valueSerde commtypes.Serde
	msgSerde   commtypes.MsgSerde
}

var _ = processor.Sink(&ShardedSharedLogStreamSink{})

func NewShardedSharedLogStreamSink(stream *ShardedSharedLogStream, config *StreamSinkConfig) *ShardedSharedLogStreamSink {
	return &ShardedSharedLogStreamSink{
		stream:     stream,
		keySerde:   config.KeySerde,
		valueSerde: config.ValueSerde,
		msgSerde:   config.MsgSerde,
	}
}

func (sls *ShardedSharedLogStreamSink) TopicName() string {
	return sls.stream.TopicName()
}

func (sls *ShardedSharedLogStreamSink) Sink(ctx context.Context, msg commtypes.Message, parNum uint8, isControl bool) error {
	if msg.Key == nil && msg.Value == nil {
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
	if bytes != nil && err == nil {
		_, err = sls.stream.Push(ctx, bytes, parNum, isControl)
		if err != nil {
			return err
		}
	}
	return err
}

func (sls *ShardedSharedLogStreamSink) KeySerde() commtypes.Serde {
	return sls.keySerde
}
