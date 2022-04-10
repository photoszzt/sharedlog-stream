package sharedlog_stream

import (
	"context"
	"os"
	"sharedlog-stream/pkg/stream/processor"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"sharedlog-stream/pkg/stream/processor/store"
)

type SharedLogStreamSink struct {
	pipe       processor.Pipe
	pctx       store.StoreContext
	keySerde   commtypes.Serde
	valueSerde commtypes.Serde
	msgSerde   commtypes.MsgSerde

	bufStream *BufferedSinkStream
	bufPush   bool
}

var _ = processor.Sink(&SharedLogStreamSink{})

type StreamSinkConfig struct {
	KeySerde   commtypes.Serde
	ValueSerde commtypes.Serde
	MsgSerde   commtypes.MsgSerde
}

func NewSharedLogStreamSink(stream *SharedLogStream, config *StreamSinkConfig) *SharedLogStreamSink {
	bufPush_str := os.Getenv("BUFPUSH")
	bufPush := false
	if bufPush_str == "true" || bufPush_str == "1" {
		bufPush = true
	}
	return &SharedLogStreamSink{
		keySerde:   config.KeySerde,
		valueSerde: config.ValueSerde,
		msgSerde:   config.MsgSerde,
		bufStream:  NewBufferedSinkStream(stream, 0),
		bufPush:    bufPush,
	}
}

func (sls *SharedLogStreamSink) TopicName() string {
	return sls.bufStream.Stream.TopicName()
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
	// debug.Fprintf(os.Stderr, "Sink: output key: %v, val: %v\n", string(keyEncoded), string(valEncoded))
	bytes, err := sls.msgSerde.Encode(keyEncoded, valEncoded)
	if err != nil {
		return err
	}
	if bytes != nil {
		if sls.bufPush {
			if !isControl {
				return sls.bufStream.BufPush(ctx, bytes)
			}
			err = sls.bufStream.Flush(ctx)
			if err != nil {
				return err
			}
			_, err = sls.bufStream.Stream.Push(ctx, bytes, 0, isControl, false)
			if err != nil {
				return err
			}
		} else {
			_, err = sls.bufStream.Stream.Push(ctx, bytes, 0, isControl, false)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (sls *SharedLogStreamSink) Process(ctx context.Context, msg commtypes.Message) error {
	return sls.Sink(ctx, msg, 0, false)
}

func (sls *SharedLogStreamSink) ProcessAndReturn(ctx context.Context, msg commtypes.Message) ([]commtypes.Message, error) {
	panic("not implemented")
}

func (sls *SharedLogStreamSink) Flush(ctx context.Context) error {
	if sls.bufPush {
		return sls.bufStream.Flush(ctx)
	}
	return nil
}
