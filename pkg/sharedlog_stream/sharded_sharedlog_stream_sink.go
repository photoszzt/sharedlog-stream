package sharedlog_stream

import (
	"context"
	"fmt"
	"hash"
	"hash/fnv"
	"os"
	"sharedlog-stream/pkg/stream/processor"
	"sharedlog-stream/pkg/stream/processor/commtypes"
)

type ShardedSharedLogStreamSink struct {
	// pipe         processor.Pipe
	// pctx         processor.ProcessorContext
	stream       *ShardedSharedLogStream
	keyEncoder   commtypes.Encoder
	valueEncoder commtypes.Encoder
	msgEncoder   commtypes.MsgEncoder
	hasher       hash.Hash64
}

var _ = processor.Sink(&ShardedSharedLogStreamSink{})

func NewShardedSharedLogStreamSink(stream *ShardedSharedLogStream, config *StreamSinkConfig) *ShardedSharedLogStreamSink {
	return &ShardedSharedLogStreamSink{
		stream:       stream,
		keyEncoder:   config.KeyEncoder,
		valueEncoder: config.ValueEncoder,
		msgEncoder:   config.MsgEncoder,
		hasher:       fnv.New64(),
	}
}

func (sls *ShardedSharedLogStreamSink) Sink(ctx context.Context, msg commtypes.Message, parNum uint8, isControl bool) error {
	if msg.Key == nil && msg.Value == nil {
		return nil
	}
	var keyEncoded []byte
	if msg.Key != nil {
		fmt.Fprintf(os.Stderr, "sls: %v, key encoder: %v\n", sls, sls.keyEncoder)
		keyEncodedTmp, err := sls.keyEncoder.Encode(msg.Key)
		if err != nil {
			return err
		}
		keyEncoded = keyEncodedTmp
	}
	valEncoded, err := sls.valueEncoder.Encode(msg.Value)
	if err != nil {
		return err
	}
	bytes, err := sls.msgEncoder.Encode(keyEncoded, valEncoded)
	if bytes != nil && err == nil {
		_, err = sls.stream.Push(ctx, bytes, parNum, isControl)
		if err != nil {
			return err
		}
	}
	return err
}
