package sharedlog_stream

import (
	"context"
	"sharedlog-stream/pkg/stream/processor"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"sharedlog-stream/pkg/stream/processor/store"
)

type SharedLogStreamSink struct {
	pipe       processor.Pipe
	pctx       store.StoreContext
	stream     *SharedLogStream
	keySerde   commtypes.Serde
	valueSerde commtypes.Serde
	msgSerde   commtypes.MsgSerde
}

var _ = processor.Sink(&SharedLogStreamSink{})

type StreamSinkConfig struct {
	KeySerde   commtypes.Serde
	ValueSerde commtypes.Serde
	MsgSerde   commtypes.MsgSerde
}

func NewSharedLogStreamSink(stream *SharedLogStream, config *StreamSinkConfig) *SharedLogStreamSink {
	return &SharedLogStreamSink{
		stream:     stream,
		keySerde:   config.KeySerde,
		valueSerde: config.ValueSerde,
		msgSerde:   config.MsgSerde,
	}
}

func (sls *SharedLogStreamSink) TopicName() string {
	return sls.stream.TopicName()
}

func (sls *SharedLogStreamSink) KeySerde() commtypes.Serde {
	return sls.keySerde
}

func (sls *SharedLogStreamSink) WithPipe(pipe processor.Pipe) {
	sls.pipe = pipe
}

func (sls *SharedLogStreamSink) WithProcessorContext(pctx store.StoreContext) {
	sls.pctx = pctx
}

func (sls *SharedLogStreamSink) Sink(ctx context.Context, msg commtypes.Message, parNum uint8, isControl bool) error {
	if msg.Key == nil && msg.Value == nil {
		return nil
	}
	var keyEncoded []byte
	if msg.Key != nil {
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
	// fmt.Fprintf(os.Stderr, "Sink: output key: %v, val: %v\n", string(keyEncoded), string(valEncoded))
	bytes, err := sls.msgSerde.Encode(keyEncoded, valEncoded)
	if bytes != nil && err == nil {
		_, err = sls.stream.Push(ctx, bytes, 0, isControl)
		if err != nil {
			return err
		}
	}
	return err
}

func (sls *SharedLogStreamSink) Process(ctx context.Context, msg commtypes.Message) error {
	return sls.Sink(ctx, msg, 0, false)
}

func (sls *SharedLogStreamSink) ProcessAndReturn(ctx context.Context, msg commtypes.Message) ([]commtypes.Message, error) {
	panic("not implemented")
}
