package sharedlog_stream

import (
	"context"
	"fmt"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"time"
)

type ShardedSharedLogStreamSource struct {
	keyDecoder   commtypes.Decoder
	valueDecoder commtypes.Decoder
	msgDecoder   commtypes.MsgDecoder
	stream       *ShardedSharedLogStream
	timeout      time.Duration
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

func (s *ShardedSharedLogStreamSource) Consume(ctx context.Context, parNum uint8) (commtypes.Message, uint64, error) {
	startTime := time.Now()
	for {
		if s.timeout != 0 && time.Since(startTime) >= s.timeout {
			break
		}
		val, seqNum, err := s.stream.ReadNext(ctx, parNum)
		if err != nil {
			if IsStreamEmptyError(err) {
				// fmt.Fprintf(os.Stderr, "stream is empty\n")
				time.Sleep(time.Duration(100) * time.Microsecond)
				continue
			} else if IsStreamTimeoutError(err) {
				// fmt.Fprintf(os.Stderr, "stream time out\n")
				continue
			} else {
				return commtypes.EmptyMessage, 0, err
			}
		}
		keyEncoded, valueEncoded, err := s.msgDecoder.Decode(val)
		if err != nil {
			return commtypes.EmptyMessage, 0, fmt.Errorf("fail to decode msg: %v", err)
		}
		key, err := s.keyDecoder.Decode(keyEncoded)
		if err != nil {
			return commtypes.EmptyMessage, 0, fmt.Errorf("fail to decode key: %v", err)
		}
		value, err := s.valueDecoder.Decode(valueEncoded)
		if err != nil {
			return commtypes.EmptyMessage, 0, fmt.Errorf("fail to decode value: %v", err)
		}
		return commtypes.Message{Key: key, Value: value}, seqNum, nil
	}
	return commtypes.EmptyMessage, 0, ErrStreamSourceTimeout
}
