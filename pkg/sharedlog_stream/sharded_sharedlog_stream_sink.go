package sharedlog_stream

import (
	"hash"
	"hash/fnv"
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

func NewShardedSharedLogStreamSink(stream *ShardedSharedLogStream, config *StreamSinkConfig) *ShardedSharedLogStreamSink {
	return &ShardedSharedLogStreamSink{
		stream:       stream,
		keyEncoder:   config.KeyEncoder,
		valueEncoder: config.ValueEncoder,
		msgEncoder:   config.MsgEncoder,
		hasher:       fnv.New64(),
	}
}

func (sls *ShardedSharedLogStreamSink) Sink(msg commtypes.Message, parNum uint8) error {
	if msg.Key == nil && msg.Value == nil {
		return nil
	}
	var keyEncoded []byte
	var additionalTag []uint64
	if msg.Key != nil {
		keyEncodedTmp, err := sls.keyEncoder.Encode(msg.Key)
		if err != nil {
			return err
		}
		keyEncoded = keyEncodedTmp
		keyTag := sls.hasher.Sum64()
		additionalTag = []uint64{keyTag}
	}
	valEncoded, err := sls.valueEncoder.Encode(msg.Value)
	if err != nil {
		return err
	}
	bytes, err := sls.msgEncoder.Encode(keyEncoded, valEncoded)
	if bytes != nil && err == nil {
		_, err = sls.stream.Push(bytes, parNum, additionalTag)
		if err != nil {
			return err
		}
	}
	return err
}
