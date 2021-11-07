package sharedlog_stream

import (
	"time"

	"sharedlog-stream/pkg/stream/processor/commtypes"

	"golang.org/x/xerrors"
)

var (
	ErrStreamSourceTimeout = xerrors.New("SharedLogStreamSource consume timeout")
)

type SharedLogStreamSource struct {
	keyDecoder   commtypes.Decoder
	valueDecoder commtypes.Decoder
	msgDecoder   commtypes.MsgDecoder
	stream       *SharedLogStream
	timeout      time.Duration
}

type SharedLogStreamConfig struct {
	KeyDecoder   commtypes.Decoder
	ValueDecoder commtypes.Decoder
	MsgDecoder   commtypes.MsgDecoder
	Timeout      time.Duration
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

func (s *SharedLogStreamSource) Consume(parNum uint8) (commtypes.Message, error) {
	startTime := time.Now()
	for {
		if s.timeout != 0 && time.Since(startTime) >= s.timeout {
			break
		}
		val, err := s.stream.ReadNext(0)
		if err != nil {
			if IsStreamEmptyError(err) {
				// fmt.Fprintf(os.Stderr, "stream is empty\n")
				time.Sleep(time.Duration(100) * time.Microsecond)
				continue
			} else if IsStreamTimeoutError(err) {
				// fmt.Fprintf(os.Stderr, "stream time out\n")
				continue
			} else {
				return commtypes.EmptyMessage, err
			}
		}
		keyEncoded, valueEncoded, err := s.msgDecoder.Decode(val)
		if err != nil {
			return commtypes.EmptyMessage, err
		}
		key, err := s.keyDecoder.Decode(keyEncoded)
		if err != nil {
			return commtypes.EmptyMessage, err
		}
		value, err := s.valueDecoder.Decode(valueEncoded)
		if err != nil {
			return commtypes.EmptyMessage, err
		}
		return commtypes.Message{Key: key, Value: value}, nil
	}
	return commtypes.EmptyMessage, ErrStreamSourceTimeout
}
