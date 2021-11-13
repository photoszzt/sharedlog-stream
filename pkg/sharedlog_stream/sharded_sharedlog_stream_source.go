package sharedlog_stream

import (
	"context"
	"fmt"
	"sharedlog-stream/pkg/errors"
	"sharedlog-stream/pkg/stream/processor"
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

var _ = processor.Source(&ShardedSharedLogStreamSource{})

func NewShardedSharedLogStreamSource(stream *ShardedSharedLogStream, config *SharedLogStreamConfig) *ShardedSharedLogStreamSource {
	return &ShardedSharedLogStreamSource{
		stream:       stream,
		timeout:      config.Timeout,
		keyDecoder:   config.KeyDecoder,
		valueDecoder: config.ValueDecoder,
		msgDecoder:   config.MsgDecoder,
	}
}

func (s *ShardedSharedLogStreamSource) Consume(ctx context.Context, parNum uint8) ([]commtypes.MsgAndSeq, error) {
	startTime := time.Now()
	var msgs []commtypes.MsgAndSeq
	for {
		if s.timeout != 0 && time.Since(startTime) >= s.timeout {
			break
		}
		_, rawMsgs, err := s.stream.ReadNext(ctx, parNum)
		if err != nil {
			if errors.IsStreamEmptyError(err) {
				// fmt.Fprintf(os.Stderr, "stream is empty\n")
				time.Sleep(time.Duration(100) * time.Microsecond)
				continue
			} else if errors.IsStreamTimeoutError(err) {
				// fmt.Fprintf(os.Stderr, "stream time out\n")
				continue
			} else {
				return nil, err
			}
		}
		for _, rawMsg := range rawMsgs {
			keyEncoded, valueEncoded, err := s.msgDecoder.Decode(rawMsg.Payload)
			if err != nil {
				return nil, fmt.Errorf("fail to decode msg: %v", err)
			}
			key, err := s.keyDecoder.Decode(keyEncoded)
			if err != nil {
				return nil, fmt.Errorf("fail to decode key: %v", err)
			}
			value, err := s.valueDecoder.Decode(valueEncoded)
			if err != nil {
				return nil, fmt.Errorf("fail to decode value: %v", err)
			}
			msgs = append(msgs, commtypes.MsgAndSeq{
				Msg: commtypes.Message{
					Key:   key,
					Value: value,
				},
				MsgSeqNum: rawMsg.MsgSeqNum,
				LogSeqNum: rawMsg.LogSeqNum,
			})
		}
		return msgs, nil
	}
	return nil, ErrStreamSourceTimeout
}
