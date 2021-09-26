package sharedlog_stream

import (
	"sharedlog-stream/pkg/stream/processor"
)

type SharedLogStreamSink struct {
	pipe         processor.Pipe
	pctx         processor.ProcessorContext
	stream       *SharedLogStream
	keyEncoder   processor.Encoder
	valueEncoder processor.Encoder
	msgEncoder   processor.MsgEncoder
}

func NewSharedLogStreamSink(stream *SharedLogStream, keyEncoder processor.Encoder, valEncoder processor.Encoder, msgEncoder processor.MsgEncoder) *SharedLogStreamSink {
	return &SharedLogStreamSink{
		stream:       stream,
		keyEncoder:   keyEncoder,
		valueEncoder: valEncoder,
		msgEncoder:   msgEncoder,
	}
}

func (sls *SharedLogStreamSink) WithPipe(pipe processor.Pipe) {
	sls.pipe = pipe
}

func (sls *SharedLogStreamSink) WithProcessorContext(pctx processor.ProcessorContext) {
	sls.pctx = pctx
}

func (sls *SharedLogStreamSink) Process(msg processor.Message) error {
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
		_, err = sls.stream.Push(bytes)
		if err != nil {
			return err
		}
	}
	return err
}
