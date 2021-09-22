package sharedlog_stream

import (
	"time"

	"sharedlog-stream/pkg/stream/processor"

	"golang.org/x/xerrors"
)

var (
	errSharedLogStreamSourceTimeout = xerrors.New("SharedLogStreamSource consume timeout")
)

type SharedLogStreamSource struct {
	stream       *SharedLogStream
	timeout      time.Duration
	keyDecoder   processor.Decoder
	valueDecoder processor.Decoder
	msgDecoder   processor.MsgDecoder
}

func NewSharedLogStreamSource(stream *SharedLogStream, timeout int, keyDecoder processor.Decoder, valueDecoder processor.Decoder,
	msgDecoder processor.MsgDecoder) *SharedLogStreamSource {
	return &SharedLogStreamSource{
		stream:       stream,
		timeout:      time.Duration(timeout) * time.Second,
		keyDecoder:   keyDecoder,
		valueDecoder: valueDecoder,
		msgDecoder:   msgDecoder,
	}
}

func (s *SharedLogStreamSource) Consume() (processor.Message, error) {
	startTime := time.Now()
	for {
		if s.timeout != 0 && time.Since(startTime) >= s.timeout {
			break
		}
		val, err := s.stream.Pop()
		if err != nil {
			if IsStreamEmptyError(err) {
				time.Sleep(time.Duration(100) * time.Microsecond)
				continue
			} else if IsStreamTimeoutError(err) {
				continue
			} else {
				return processor.EmptyMessage, err
			}
		}
		keyEncoded, valueEncoded, err := s.msgDecoder.Decode(val)
		if err != nil {
			return processor.EmptyMessage, err
		}
		key, err := s.keyDecoder.Decode(keyEncoded)
		if err != nil {
			return processor.EmptyMessage, err
		}
		value, err := s.valueDecoder.Decode(valueEncoded)
		if err != nil {
			return processor.EmptyMessage, err
		}
		return processor.Message{Key: key, Value: value}, nil
	}
	return processor.EmptyMessage, errSharedLogStreamSourceTimeout
}
