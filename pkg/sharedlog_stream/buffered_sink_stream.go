package sharedlog_stream

import (
	"context"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"sync"
)

const (
	SINK_BUFFER_MAX_ENTRY = 10000
	SINK_BUFFER_MAX_SIZE  = 131072
	MSG_CHAN_SIZE         = 10000
)

var (
	DEFAULT_PAYLOAD_ARR_SERDE = commtypes.PayloadArrMsgpSerde{}
)

type BufferedSinkStream struct {
	sinkMu     sync.Mutex
	sinkBuffer [][]byte

	Stream          *SharedLogStream
	payloadArrSerde commtypes.Serde
	currentSize     int
	parNum          uint8
}

func NewBufferedSinkStream(stream *SharedLogStream, parNum uint8) *BufferedSinkStream {
	return &BufferedSinkStream{
		sinkBuffer:      make([][]byte, 0, SINK_BUFFER_MAX_ENTRY),
		payloadArrSerde: DEFAULT_PAYLOAD_ARR_SERDE,
		parNum:          parNum,
		Stream:          stream,
		currentSize:     0,
	}
}

// don't mix the nolock version and goroutine safe version

func (s *BufferedSinkStream) BufPushNoLock(ctx context.Context, payload []byte, taskId uint64, taskEpoch uint16, transactionID uint64) error {
	payload_size := len(payload)
	if len(s.sinkBuffer) < SINK_BUFFER_MAX_ENTRY && s.currentSize+payload_size < SINK_BUFFER_MAX_SIZE {
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
		_, err = s.Stream.Push(ctx, payloads, s.parNum, false, true, taskId, taskEpoch, transactionID)
		if err != nil {
			return err
		}
		s.sinkBuffer = make([][]byte, 0, SINK_BUFFER_MAX_ENTRY)
		s.sinkBuffer = append(s.sinkBuffer, payload)
		s.currentSize = payload_size
	}
	return nil
}

func (s *BufferedSinkStream) FlushNoLock(ctx context.Context, taskId uint64, taskEpoch uint16, transactionID uint64) error {
	if len(s.sinkBuffer) != 0 {
		payloadArr := &commtypes.PayloadArr{
			Payloads: s.sinkBuffer,
		}
		payloads, err := s.payloadArrSerde.Encode(payloadArr)
		if err != nil {
			return err
		}
		_, err = s.Stream.Push(ctx, payloads, s.parNum, false, true, taskId, taskEpoch, transactionID)
		if err != nil {
			return err
		}
		s.sinkBuffer = make([][]byte, 0, SINK_BUFFER_MAX_ENTRY)
	}
	return nil
}

func (s *BufferedSinkStream) BufPushGoroutineSafe(ctx context.Context, payload []byte,
	taskId uint64, taskEpoch uint16, transactionID uint64,
) error {
	s.sinkMu.Lock()
	defer s.sinkMu.Unlock()
	payload_size := len(payload)
	if len(s.sinkBuffer) < SINK_BUFFER_MAX_ENTRY && s.currentSize+payload_size < SINK_BUFFER_MAX_SIZE {
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
		_, err = s.Stream.Push(ctx, payloads, s.parNum, false, true, taskId, taskEpoch, transactionID)
		if err != nil {
			return err
		}
		s.sinkBuffer = make([][]byte, 0, SINK_BUFFER_MAX_ENTRY)
		s.sinkBuffer = append(s.sinkBuffer, payload)
		s.currentSize = payload_size
	}
	return nil
}

func (s *BufferedSinkStream) FlushGoroutineSafe(ctx context.Context, taskId uint64, taskEpoch uint16, transactionID uint64) error {
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
		_, err = s.Stream.Push(ctx, payloads, s.parNum, false, true, taskId, taskEpoch, transactionID)
		if err != nil {
			return err
		}
		s.sinkBuffer = make([][]byte, 0, SINK_BUFFER_MAX_ENTRY)
	}
	return nil
}
