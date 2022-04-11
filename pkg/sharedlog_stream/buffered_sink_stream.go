package sharedlog_stream

import (
	"context"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"sync"

	"cs.utexas.edu/zjia/faas/protocol"
)

const (
	SINK_BUFFER_SIZE = 50
	// besides payload there're extra overhead
	SINK_BUFFER_MAX_SIZE = protocol.MessageInlineDataSize - 512
)

// not goroutine safe
type BufferedSinkStream struct {
	sinkMu     sync.Mutex
	sinkBuffer [][]byte

	Stream          *SharedLogStream
	payloadArrSerde commtypes.Serde
	parNum          uint8
	currentSize     int
}

func NewBufferedSinkStream(stream *SharedLogStream, parNum uint8) *BufferedSinkStream {
	payloadArrSerde := commtypes.PayloadArrMsgpSerde{}
	return &BufferedSinkStream{
		sinkBuffer:      make([][]byte, 0, SINK_BUFFER_SIZE),
		payloadArrSerde: payloadArrSerde,
		parNum:          parNum,
		Stream:          stream,
		currentSize:     0,
	}
}

func (s *BufferedSinkStream) BufPush(ctx context.Context, payload []byte) error {
	s.sinkMu.Lock()
	defer s.sinkMu.Unlock()
	payload_size := len(payload)
	if len(s.sinkBuffer) < SINK_BUFFER_SIZE && s.currentSize+payload_size < SINK_BUFFER_MAX_SIZE {
		s.sinkBuffer = append(s.sinkBuffer, payload)
		s.currentSize += payload_size
	} else {
		payloadArr := &commtypes.PayloadArr{
			Payloads: s.sinkBuffer,
		}
		payloads, err := s.payloadArrSerde.Encode(payloadArr)
		if err != nil {
			return err
		}
		_, err = s.Stream.Push(ctx, payloads, s.parNum, false, true)
		if err != nil {
			return err
		}
		s.sinkBuffer = make([][]byte, 0, SINK_BUFFER_SIZE)
		s.sinkBuffer = append(s.sinkBuffer, payload)
		s.currentSize = payload_size
	}
	return nil
}

func (s *BufferedSinkStream) Flush(ctx context.Context) error {
	s.sinkMu.Lock()
	defer s.sinkMu.Unlock()
	if len(s.sinkBuffer) != 0 {
		payloadArr := &commtypes.PayloadArr{
			Payloads: s.sinkBuffer,
		}
		payloads, err := s.payloadArrSerde.Encode(payloadArr)
		if err != nil {
			return err
		}
		_, err = s.Stream.Push(ctx, payloads, s.parNum, false, true)
		if err != nil {
			return err
		}
		s.sinkBuffer = make([][]byte, 0, SINK_BUFFER_SIZE)
	}
	return nil
}
