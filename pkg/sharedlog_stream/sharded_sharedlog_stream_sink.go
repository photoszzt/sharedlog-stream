package sharedlog_stream

import "sharedlog-stream/pkg/stream/processor"

type ShardedSharedLogStreamSink struct {
	// pipe         processor.Pipe
	// pctx         processor.ProcessorContext
	stream       *ShardedSharedLogStream
	keyEncoder   processor.Encoder
	valueEncoder processor.Encoder
	msgEncoder   processor.MsgEncoder
}

func NewShardedSharedLogStreamSink(stream *ShardedSharedLogStream, config *StreamSinkConfig) *ShardedSharedLogStreamSink {
	return &ShardedSharedLogStreamSink{
		stream:       stream,
		keyEncoder:   config.KeyEncoder,
		valueEncoder: config.ValueEncoder,
		msgEncoder:   config.MsgEncoder,
	}
}

func (sls *ShardedSharedLogStreamSink) Sink(msg processor.Message, parNum uint8) error {
	if msg.Key == nil && msg.Value == nil {
		return nil
	}
	var keyEncoded []byte
	if msg.Key != nil {
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
	// fmt.Fprintf(os.Stderr, "Sink: output key: %v, val: %v\n", string(keyEncoded), string(valEncoded))
	bytes, err := sls.msgEncoder.Encode(keyEncoded, valEncoded)
	if bytes != nil && err == nil {
		_, err = sls.stream.Push(bytes, parNum)
		if err != nil {
			return err
		}
	}
	return err
}
