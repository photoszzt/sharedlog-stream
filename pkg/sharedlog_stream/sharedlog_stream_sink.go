package sharedlog_stream

import (
	"cs.utexas.edu/zhitingz/sharedlog-stream/pkg/stream/processor"
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
		stream: stream,
	}
}

func (sls *SharedLogStreamSink) WithPipe(pipe processor.Pipe) {
	sls.pipe = pipe
}

func (sls *SharedLogStreamSink) WithProcessorContext(pctx processor.ProcessorContext) {
	sls.pctx = pctx
}

func (sls *SharedLogStreamSink) Process(msg processor.Message) error {
	// ignore the key now
	keyEncoded, err := sls.keyEncoder.Encode(msg.Key)
	if err != nil {
		return err
	}
	valEncoded, err := sls.valueEncoder.Encode(msg.Value)
	if err != nil {
		return err
	}
	bytes, err := sls.msgEncoder.Encode(keyEncoded, valEncoded)
	if bytes != nil && err == nil {
		_, err = sls.stream.Push(bytes)
		if err != nil {
			return err
		}
	}
	return err
}
