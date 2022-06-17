package sharedlog_stream

import (
	"context"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/exactly_once_intr"
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

	payloadArrSerde commtypes.Serde
	Stream          *SharedLogStream

	once               sync.Once
	initialProdInEpoch uint64
	currentProdInEpoch uint64
	currentSize        int
	guarantee          exactly_once_intr.GuaranteeMth
	parNum             uint8
}

func NewBufferedSinkStream(stream *SharedLogStream, parNum uint8) *BufferedSinkStream {
	return &BufferedSinkStream{
		sinkBuffer:      make([][]byte, 0, SINK_BUFFER_MAX_ENTRY),
		payloadArrSerde: DEFAULT_PAYLOAD_ARR_SERDE,
		parNum:          parNum,
		Stream:          stream,
		currentSize:     0,
		once:            sync.Once{},
		guarantee:       exactly_once_intr.AT_LEAST_ONCE,
	}
}

// don't mix the nolock version and goroutine safe version

func (s *BufferedSinkStream) BufPushNoLock(ctx context.Context, payload []byte, producerId exactly_once_intr.ProducerId) error {
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
		seqNum, err := s.Stream.Push(ctx, payloads, s.parNum, StreamEntryMeta(false, true), producerId)
		if err != nil {
			return err
		}
		s.updateProdSeqNum(seqNum)
		s.sinkBuffer = make([][]byte, 0, SINK_BUFFER_MAX_ENTRY)
		s.sinkBuffer = append(s.sinkBuffer, payload)
		s.currentSize = payload_size
	}
	return nil
}

func (s *BufferedSinkStream) updateProdSeqNum(seqNum uint64) {
	if s.guarantee == exactly_once_intr.EPOCH_MARK {
		s.once.Do(func() {
			s.initialProdInEpoch = seqNum
		})
		s.currentProdInEpoch = seqNum
	}
}

func (s *BufferedSinkStream) ExactlyOnce(gua exactly_once_intr.GuaranteeMth) {
	s.guarantee = gua
}

func (s *BufferedSinkStream) ResetInitialProd() {
	if s.guarantee == exactly_once_intr.EPOCH_MARK {
		s.once = sync.Once{}
	}
}

func (s *BufferedSinkStream) GetInitialProdSeqNum() uint64 {
	return s.initialProdInEpoch
}

func (s *BufferedSinkStream) GetCurrentProdSeqNum() uint64 {
	return s.currentProdInEpoch
}

func (s *BufferedSinkStream) FlushNoLock(ctx context.Context, producerId exactly_once_intr.ProducerId) error {
	if len(s.sinkBuffer) != 0 {
		payloadArr := &commtypes.PayloadArr{
			Payloads: s.sinkBuffer,
		}
		payloads, err := s.payloadArrSerde.Encode(payloadArr)
		if err != nil {
			return err
		}
		seqNum, err := s.Stream.Push(ctx, payloads, s.parNum, StreamEntryMeta(false, true), producerId)
		if err != nil {
			return err
		}
		s.updateProdSeqNum(seqNum)
		s.sinkBuffer = make([][]byte, 0, SINK_BUFFER_MAX_ENTRY)
	}
	return nil
}

func (s *BufferedSinkStream) BufPushGoroutineSafe(ctx context.Context, payload []byte, producerId exactly_once_intr.ProducerId,
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
		seqNum, err := s.Stream.Push(ctx, payloads, s.parNum, StreamEntryMeta(false, true), producerId)
		if err != nil {
			return err
		}
		s.updateProdSeqNum(seqNum)
		s.sinkBuffer = make([][]byte, 0, SINK_BUFFER_MAX_ENTRY)
		s.sinkBuffer = append(s.sinkBuffer, payload)
		s.currentSize = payload_size
	}
	return nil
}

func (s *BufferedSinkStream) FlushGoroutineSafe(ctx context.Context, producerId exactly_once_intr.ProducerId) error {
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
		seqNum, err := s.Stream.Push(ctx, payloads, s.parNum, StreamEntryMeta(false, true), producerId)
		if err != nil {
			return err
		}
		s.updateProdSeqNum(seqNum)
		s.sinkBuffer = make([][]byte, 0, SINK_BUFFER_MAX_ENTRY)
	}
	return nil
}
