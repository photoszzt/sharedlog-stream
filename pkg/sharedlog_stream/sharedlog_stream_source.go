package sharedlog_stream

import (
	"time"

	"sharedlog-stream/pkg/stream/processor"

	"golang.org/x/xerrors"
)

var (
	ErrStreamSourceTimeout = xerrors.New("SharedLogStreamSource consume timeout")
)

type SharedLogStreamSource struct {
	stream       *SharedLogStream
	timeout      time.Duration
	keyDecoder   processor.Decoder
	valueDecoder processor.Decoder
	msgDecoder   processor.MsgDecoder
}

type SharedLogStreamConfig struct {
	Timeout      time.Duration
	KeyDecoder   processor.Decoder
	ValueDecoder processor.Decoder
	MsgDecoder   processor.MsgDecoder
}

func NewSharedLogStreamSource(stream *SharedLogStream, config *SharedLogStreamConfig) *SharedLogStreamSource {
	return &SharedLogStreamSource{
		stream:       stream,
		timeout:      config.Timeout,
		keyDecoder:   config.KeyDecoder,
		valueDecoder: config.ValueDecoder,
		msgDecoder:   config.MsgDecoder,
	}
}

func (s *SharedLogStreamSource) Consume() (processor.Message, error) {
	startTime := time.Now()
	for {
		if s.timeout != 0 && time.Since(startTime) >= s.timeout {
			break
		}
		val, err := s.stream.Pop(0)
		if err != nil {
			if IsStreamEmptyError(err) {
				// fmt.Fprintf(os.Stderr, "stream is empty\n")
				time.Sleep(time.Duration(100) * time.Microsecond)
				continue
			} else if IsStreamTimeoutError(err) {
				// fmt.Fprintf(os.Stderr, "stream time out\n")
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
	return processor.EmptyMessage, ErrStreamSourceTimeout
}
