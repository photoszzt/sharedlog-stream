package sharedlog_stream

import (
	"context"
	"time"

	"sharedlog-stream/pkg/errors"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"sharedlog-stream/pkg/stream/processor/store"
)

var ()

type SharedLogStreamSource struct {
	keyDecoder   commtypes.Decoder
	valueDecoder commtypes.Decoder
	msgDecoder   commtypes.MsgDecoder
	stream       *SharedLogStream
	timeout      time.Duration
}

var _ = store.Stream(&SharedLogStream{})

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

func (s *SharedLogStreamSource) Stream() store.Stream {
	return s.stream
}

func (s *SharedLogStreamSource) TopicName() string {
	return s.stream.topicName
}

func (s *SharedLogStreamSource) SetCursor(cursor uint64, parNum uint8) {
	s.stream.SetCursor(cursor, parNum)
}

func (s *SharedLogStreamSource) Consume(ctx context.Context, parNum uint8) ([]commtypes.MsgAndSeq, error) {
	startTime := time.Now()
	var msgs []commtypes.MsgAndSeq
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
			keyEncoded, valueEncoded, err := s.msgDecoder.Decode(rawMsg.Payload)
			if err != nil {
				return nil, err
			}
			var key interface{}
			if keyEncoded != nil {
				key, err = s.keyDecoder.Decode(keyEncoded)
				if err != nil {
					return nil, err
				}
			}
			// debug.Fprintf(os.Stderr, "val encoded is %v\n", valueEncoded)
			value, err := s.valueDecoder.Decode(valueEncoded)
			if err != nil {
				return nil, err
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
	return nil, errors.ErrStreamSourceTimeout
}
