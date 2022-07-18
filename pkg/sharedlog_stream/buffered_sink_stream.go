package sharedlog_stream

import (
	"context"
	"fmt"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/debug"
	"sharedlog-stream/pkg/exactly_once_intr"
	"sharedlog-stream/pkg/stats"
	"sharedlog-stream/pkg/utils/syncutils"
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
	mux        syncutils.Mutex
	sinkBuffer [][]byte

	payloadArrSerde commtypes.Serde
	Stream          *SharedLogStream

	bufferEntryStats stats.IntCollector
	bufferSizeStats  stats.IntCollector

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
		bufferEntryStats: stats.NewIntCollector(fmt.Sprintf("bufEntry_%v", parNum),
			stats.DEFAULT_COLLECT_DURATION),
		bufferSizeStats: stats.NewIntCollector(fmt.Sprintf("bufSize_%v", parNum),
			stats.DEFAULT_COLLECT_DURATION),
	}
}

// don't mix the nolock version and goroutine safe version

func (s *BufferedSinkStream) BufPushNoLock(ctx context.Context, payload []byte, producerId commtypes.ProducerId) error {
	payload_size := len(payload)
	if len(s.sinkBuffer) < SINK_BUFFER_MAX_ENTRY && s.currentSize+payload_size < SINK_BUFFER_MAX_SIZE {
		s.sinkBuffer = append(s.sinkBuffer, payload)
		s.currentSize += payload_size
	} else {
		s.bufferEntryStats.AddSample(len(s.sinkBuffer))
		s.bufferSizeStats.AddSample(s.currentSize)
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

func (s *BufferedSinkStream) BufPushGoroutineSafe(ctx context.Context, payload []byte, producerId commtypes.ProducerId) error {
	s.mux.Lock()
	defer s.mux.Unlock()
	return s.BufPushNoLock(ctx, payload, producerId)
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

func (s *BufferedSinkStream) FlushNoLock(ctx context.Context, producerId commtypes.ProducerId) error {
	if len(s.sinkBuffer) != 0 {
		s.bufferEntryStats.AddSample(len(s.sinkBuffer))
		s.bufferSizeStats.AddSample(s.currentSize)
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
		s.currentSize = 0
	}
	debug.Assert(len(s.sinkBuffer) == 0 && s.currentSize == 0,
		"sink buffer should be empty after flush")
	return nil
}

func (s *BufferedSinkStream) FlushGoroutineSafe(ctx context.Context, producerId commtypes.ProducerId) error {
	s.mux.Lock()
	defer s.mux.Unlock()
	return s.FlushNoLock(ctx, producerId)
}
