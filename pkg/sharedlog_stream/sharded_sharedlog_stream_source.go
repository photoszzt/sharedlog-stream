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

type StreamSourceConfig struct {
	KeyDecoder   commtypes.Decoder
	ValueDecoder commtypes.Decoder
	MsgDecoder   commtypes.MsgDecoder
	Timeout      time.Duration
}

type ShardedSharedLogStreamSource struct {
	keyDecoder      commtypes.Decoder
	valueDecoder    commtypes.Decoder
	msgDecoder      commtypes.MsgDecoder
	payloadArrSerde commtypes.Serde
	stream          *ShardedSharedLogStream
	timeout         time.Duration
}

var _ = processor.Source(&ShardedSharedLogStreamSource{})

func NewShardedSharedLogStreamSource(stream *ShardedSharedLogStream, config *StreamSourceConfig) *ShardedSharedLogStreamSource {
	return &ShardedSharedLogStreamSource{
		stream:          stream,
		timeout:         config.Timeout,
		keyDecoder:      config.KeyDecoder,
		valueDecoder:    config.ValueDecoder,
		msgDecoder:      config.MsgDecoder,
		payloadArrSerde: commtypes.PayloadArrMsgpSerde{},
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

func (s *ShardedSharedLogStreamSource) Consume(ctx context.Context, parNum uint8) (*commtypes.MsgAndSeqs, error) {
	startTime := time.Now()
	var msgs []commtypes.MsgAndSeq
	totalLen := uint32(0)
L:
	for {
		select {
		case <-ctx.Done():
			break L
		default:
		}
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
					MsgArr:    nil,
					MsgSeqNum: rawMsg.MsgSeqNum,
					LogSeqNum: rawMsg.LogSeqNum,
					IsControl: true,
				})
				totalLen += 1
				continue
			}
			if rawMsg.IsPayloadArr {
				payloadArrTmp, err := s.payloadArrSerde.Decode(rawMsg.Payload)
				if err != nil {
					return nil, err
				}
				payloadArr := payloadArrTmp.(commtypes.PayloadArr)
				var msgArr []commtypes.Message
				for _, payload := range payloadArr.Payloads {
					key, val, err := decodePayload(payload, s.msgDecoder, s.keyDecoder, s.valueDecoder)
					if err != nil {
						return nil, err
					}
					msgArr = append(msgArr, commtypes.Message{Key: key, Value: val})
				}
				totalLen += uint32(len(msgArr))
				msgs = append(msgs, commtypes.MsgAndSeq{MsgArr: msgArr, Msg: commtypes.EmptyMessage,
					MsgSeqNum: rawMsg.MsgSeqNum, LogSeqNum: rawMsg.LogSeqNum})
			} else {
				key, value, err := decodePayload(rawMsg.Payload, s.msgDecoder, s.keyDecoder, s.valueDecoder)
				if err != nil {
					return nil, err
				}
				msgs = append(msgs, commtypes.MsgAndSeq{
					Msg:       commtypes.Message{Key: key, Value: value},
					MsgArr:    nil,
					MsgSeqNum: rawMsg.MsgSeqNum,
					LogSeqNum: rawMsg.LogSeqNum,
					IsControl: false,
				})
				totalLen += 1
			}
		}
		return &commtypes.MsgAndSeqs{Msgs: msgs, TotalLen: totalLen}, nil
	}
	return nil, errors.ErrStreamSourceTimeout
}

func decodePayload(payload []byte,
	msgDecoder commtypes.MsgDecoder,
	keyDecoder commtypes.Decoder,
	valueDecoder commtypes.Decoder,
) (interface{}, interface{}, error) {
	keyEncoded, valueEncoded, err := msgDecoder.Decode(payload)
	if err != nil {
		return nil, nil, fmt.Errorf("fail to decode msg: %v", err)
	}
	key, err := keyDecoder.Decode(keyEncoded)
	if err != nil {
		return nil, nil, fmt.Errorf("fail to decode key: %v", err)
	}
	value, err := valueDecoder.Decode(valueEncoded)
	if err != nil {
		return nil, nil, fmt.Errorf("fail to decode value: %v", err)
	}
	return key, value, nil
}
