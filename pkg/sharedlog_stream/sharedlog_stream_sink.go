package sharedlog_stream

import (
	"cs.utexas.edu/zhitingz/sharedlog-stream/pkg/stream/processor"
)

type SharedLogStreamSink struct {
	pipe       processor.Pipe
	stream     *SharedLogStream
	msgEncoder Encoder
}

func NewSharedLogStreamSink(stream *SharedLogStream) *SharedLogStreamSink {
	return &SharedLogStreamSink{
		stream: stream,
	}
}

func (sls *SharedLogStreamSink) WithPipe(pipe processor.Pipe) {
	sls.pipe = pipe
}

func (sls *SharedLogStreamSink) Process(msg processor.Message) error {
	// ignore the key now
	bytes, err := sls.msgEncoder.Encode(msg)
	if err != nil {
		return err
	}
	if bytes != nil {
		_, err = sls.stream.Push(bytes)
		if err != nil {
			return err
		}
	}
	return nil
}
