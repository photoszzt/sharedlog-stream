package sharedlog_stream

import (
	"context"
	"fmt"
	"time"

	"sharedlog-stream/pkg/errors"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"sharedlog-stream/pkg/stream/processor/store"
)

var ()

type SharedLogStreamSource struct {
	keyDecoder      commtypes.Decoder
	valueDecoder    commtypes.Decoder
	msgDecoder      commtypes.MsgDecoder
	payloadArrSerde commtypes.Serde
	stream          *SharedLogStream
	timeout         time.Duration
}

var _ = store.Stream(&SharedLogStream{})

type StreamSourceConfig struct {
	KeyDecoder   commtypes.Decoder
	ValueDecoder commtypes.Decoder
	MsgDecoder   commtypes.MsgDecoder
	Timeout      time.Duration
}

func NewSharedLogStreamSource(stream *SharedLogStream, config *StreamSourceConfig) *SharedLogStreamSource {
	return &SharedLogStreamSource{
		stream:          stream,
		timeout:         config.Timeout,
		keyDecoder:      config.KeyDecoder,
		valueDecoder:    config.ValueDecoder,
		msgDecoder:      config.MsgDecoder,
		payloadArrSerde: commtypes.PayloadArrMsgpSerde{},
	}
}

func (s *SharedLogStreamSource) Stream() store.Stream {
	return s.stream
}

func (s *SharedLogStreamSource) TopicName() string {
	return s.stream.topicName
}

func (s *SharedLogStreamSource) SetCursor(cursor uint64, parNum uint8) {
	s.stream.SetCursor(cursor, parNum)
}

func (s *SharedLogStreamSource) Consume(ctx context.Context, parNum uint8) (*commtypes.MsgAndSeqs, error) {
	startTime := time.Now()
	var msgs []commtypes.MsgAndSeq
	totalLen := uint32(0)
	for {
		if s.timeout != 0 && time.Since(startTime) >= s.timeout {
			break
		}
		_, rawMsgs, err := s.stream.ReadNext(ctx, 0)
		if err != nil {
			if errors.IsStreamEmptyError(err) {
				// debug.Fprintf(os.Stderr, "stream is empty\n")
				time.Sleep(time.Duration(100) * time.Microsecond)
				continue
			} else if errors.IsStreamTimeoutError(err) {
				// debug.Fprintf(os.Stderr, "stream time out\n")
				break
			} else {
				return nil, err
			}
		}
		for _, rawMsg := range rawMsgs {
			if rawMsg.IsPayloadArr {
				payloadArrTmp, err := s.payloadArrSerde.Decode(rawMsg.Payload)
				if err != nil {
					return nil, err
				}
				payloadArr := payloadArrTmp.(commtypes.PayloadArr)
				var msgArr []commtypes.Message
				for _, payload := range payloadArr.Payloads {
					key, value, err := decodePayload(payload, s.msgDecoder, s.keyDecoder, s.valueDecoder)
					if err != nil {
						return nil, err
					}
					msgArr = append(msgArr, commtypes.Message{Key: key, Value: value})
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
