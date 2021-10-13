package sharedlog_stream

import (
	"fmt"
	"sharedlog-stream/pkg/stream/processor"
	"time"
)

type ShardedSharedLogStreamSource struct {
	stream       *ShardedSharedLogStream
	timeout      time.Duration
	keyDecoder   processor.Decoder
	valueDecoder processor.Decoder
	msgDecoder   processor.MsgDecoder
}

func NewShardedSharedLogStreamSource(stream *ShardedSharedLogStream, config *SharedLogStreamConfig) *ShardedSharedLogStreamSource {
	return &ShardedSharedLogStreamSource{
		stream:       stream,
		timeout:      config.Timeout,
		keyDecoder:   config.KeyDecoder,
		valueDecoder: config.ValueDecoder,
		msgDecoder:   config.MsgDecoder,
	}
}

func (s *ShardedSharedLogStreamSource) Consume(parNum uint8) (processor.Message, error) {
	startTime := time.Now()
	for {
		if s.timeout != 0 && time.Since(startTime) >= s.timeout {
			break
		}
		val, err := s.stream.Pop(parNum)
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
			return processor.EmptyMessage, fmt.Errorf("fail to decode msg: %v", err)
		}
		key, err := s.keyDecoder.Decode(keyEncoded)
		if err != nil {
			return processor.EmptyMessage, fmt.Errorf("fail to decode key: %v", err)
		}
		value, err := s.valueDecoder.Decode(valueEncoded)
		if err != nil {
			return processor.EmptyMessage, fmt.Errorf("fail to decode value: %v", err)
		}
		return processor.Message{Key: key, Value: value}, nil
	}
	return processor.EmptyMessage, ErrStreamSourceTimeout
}
