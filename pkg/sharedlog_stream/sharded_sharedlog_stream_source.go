package sharedlog_stream

import (
	"context"
	"fmt"
	"sharedlog-stream/pkg/errors"
	"sharedlog-stream/pkg/stream/processor"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"sharedlog-stream/pkg/stream/processor/store"
	"time"
)

type ScaleEpochAndBytes struct {
	Payload    []uint8
	ScaleEpoch uint64
}

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

func (s *ShardedSharedLogStreamSource) Stream() store.Stream {
	return s.stream
}

func (s *ShardedSharedLogStreamSource) TopicName() string {
	return s.stream.topicName
}

func (s *ShardedSharedLogStreamSource) SetCursor(cursor uint64, parNum uint8) {
	s.stream.SetCursor(cursor, parNum)
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
				// debug.Fprintf(os.Stderr, "stream is empty\n")
				time.Sleep(time.Duration(100) * time.Microsecond)
				continue
			} else if errors.IsStreamTimeoutError(err) {
				// debug.Fprintf(os.Stderr, "stream time out\n")
				continue
			} else {
				return nil, err
			}
		}
		for _, rawMsg := range rawMsgs {
			if !rawMsg.IsControl && len(rawMsg.Payload) == 0 {
				continue
			}
			if rawMsg.IsControl {
				msgs = append(msgs, commtypes.MsgAndSeq{
					Msg: commtypes.Message{
						Key: commtypes.SCALE_FENCE_KEY,
						Value: ScaleEpochAndBytes{
							ScaleEpoch: rawMsg.ScaleEpoch,
							Payload:    rawMsg.Payload,
						},
					},
					MsgSeqNum: rawMsg.MsgSeqNum,
					LogSeqNum: rawMsg.LogSeqNum,
					IsControl: true,
				})
				continue
			}
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
				IsControl: false,
			})
		}
		return msgs, nil
	}
	return nil, errors.ErrStreamSourceTimeout
}
