package sharedlog_stream

import (
	"context"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"sync"
)

// not goroutine safe
type BufferedSinkStream struct {
	payloadArrSerde commtypes.Serde

	sinkMu     sync.Mutex
	sinkBuffer [][]byte
	Stream     *SharedLogStream
	parNum     uint8
}

func NewBufferedSinkStream(stream *SharedLogStream, parNum uint8) *BufferedSinkStream {
	payloadArrSerde := commtypes.PayloadArrMsgpSerde{}
	return &BufferedSinkStream{
		sinkBuffer:      make([][]byte, 0, SINK_BUFFER_SIZE),
		payloadArrSerde: payloadArrSerde,
		parNum:          parNum,
		Stream:          stream,
	}
}

func (s *BufferedSinkStream) BufPush(ctx context.Context, payload []byte) error {
	s.sinkMu.Lock()
	defer s.sinkMu.Unlock()
	if len(s.sinkBuffer) < SINK_BUFFER_SIZE {
		s.sinkBuffer = append(s.sinkBuffer, payload)
	}
	if len(s.sinkBuffer) == SINK_BUFFER_SIZE {
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
