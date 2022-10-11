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
	DEFAULT_PAYLOAD_ARR_SERDE  = commtypes.PayloadArrMsgpSerde{}
	DEFAULT_PAYLOAD_ARR_SERDEG = commtypes.PayloadArrMsgpSerdeG{}
)

type BufferedSinkStream struct {
	mux        syncutils.Mutex
	sinkBuffer [][]byte

	payloadArrSerde commtypes.SerdeG[commtypes.PayloadArr]
	Stream          *SharedLogStream

	bufferEntryStats stats.StatsCollector[int]
	bufferSizeStats  stats.StatsCollector[int]

	once                 sync.Once
	initialProdInEpoch   uint64
	sink_buffer_max_size int
	currentSize          int
	guarantee            exactly_once_intr.GuaranteeMth
	parNum               uint8
}

func NewBufferedSinkStream(stream *SharedLogStream, parNum uint8) *BufferedSinkStream {
	return &BufferedSinkStream{
		sinkBuffer:      make([][]byte, 0, SINK_BUFFER_MAX_ENTRY),
		payloadArrSerde: DEFAULT_PAYLOAD_ARR_SERDEG,
		parNum:          parNum,
		Stream:          stream,
		currentSize:     0,
		once:            sync.Once{},
		guarantee:       exactly_once_intr.AT_LEAST_ONCE,
		bufferEntryStats: stats.NewStatsCollector[int](fmt.Sprintf("%s_bufEntry_%v", stream.topicName, parNum),
			stats.DEFAULT_COLLECT_DURATION),
		bufferSizeStats: stats.NewStatsCollector[int](fmt.Sprintf("%s_bufSize_%v", stream.topicName, parNum),
			stats.DEFAULT_COLLECT_DURATION),
		sink_buffer_max_size: SINK_BUFFER_MAX_SIZE,
	}
}

func NewBufferedSinkStreamWithBufferSize(stream *SharedLogStream, sinkBufferSize int, parNum uint8) *BufferedSinkStream {
	bss := NewBufferedSinkStream(stream, parNum)
	bss.sink_buffer_max_size = sinkBufferSize
	return bss
}

// don't mix the nolock version and goroutine safe version

func (s *BufferedSinkStream) BufPushNoLock(ctx context.Context, payload []byte, producerId commtypes.ProducerId) error {
	// debug.Fprintf(os.Stderr, "%s(%d) bufpush payload: %d bytes\n", s.Stream.topicName, s.parNum, len(payload))
	payload_size := len(payload)
	if len(s.sinkBuffer) < SINK_BUFFER_MAX_ENTRY && s.currentSize+payload_size < s.sink_buffer_max_size {
		s.sinkBuffer = append(s.sinkBuffer, payload)
		s.currentSize += payload_size
	} else {
		s.bufferEntryStats.AddSample(len(s.sinkBuffer))
		s.bufferSizeStats.AddSample(s.currentSize)
		payloadArr := commtypes.PayloadArr{
			Payloads: s.sinkBuffer,
		}
		payloads, err := s.payloadArrSerde.Encode(payloadArr)
		if err != nil {
			return err
		}
		seqNum, err := s.Stream.Push(ctx, payloads, s.parNum, ArrRecordMeta, producerId)
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
	payload_size := len(payload)
	if len(s.sinkBuffer) < SINK_BUFFER_MAX_ENTRY && s.currentSize+payload_size < s.sink_buffer_max_size {
		s.sinkBuffer = append(s.sinkBuffer, payload)
		s.currentSize += payload_size
		s.mux.Unlock()
	} else {
		s.bufferEntryStats.AddSample(len(s.sinkBuffer))
		s.bufferSizeStats.AddSample(s.currentSize)
		payloadArr := commtypes.PayloadArr{
			Payloads: s.sinkBuffer,
		}
		payloads, err := s.payloadArrSerde.Encode(payloadArr)
		if err != nil {
			s.mux.Unlock()
			return err
		}
		seqNum, err := s.Stream.Push(ctx, payloads, s.parNum, ArrRecordMeta, producerId)
		if err != nil {
			s.mux.Unlock()
			return err
		}
		s.updateProdSeqNum(seqNum)
		s.sinkBuffer = make([][]byte, 0, SINK_BUFFER_MAX_ENTRY)
		s.sinkBuffer = append(s.sinkBuffer, payload)
		s.currentSize = payload_size
		s.mux.Unlock()
	}
	return nil
}

func (s *BufferedSinkStream) updateProdSeqNum(seqNum uint64) {
	if s.guarantee == exactly_once_intr.EPOCH_MARK {
		s.once.Do(func() {
			s.initialProdInEpoch = seqNum
		})
	}
}

func (s *BufferedSinkStream) Push(ctx context.Context, payload []byte, parNum uint8,
	meta LogEntryMeta, producerId commtypes.ProducerId,
) (uint64, error) {
	seqNum, err := s.Stream.Push(ctx, payload, parNum, meta, producerId)
	if err != nil {
		s.mux.Unlock()
		return 0, err
	}
	s.updateProdSeqNum(seqNum)
	return seqNum, nil
}

func (s *BufferedSinkStream) PushWithTag(ctx context.Context, payload []byte, parNum uint8, tags []uint64,
	additionalTopic []string, meta LogEntryMeta, producerId commtypes.ProducerId,
) (uint64, error) {
	seqNum, err := s.Stream.PushWithTag(ctx, payload, parNum, tags, additionalTopic, meta, producerId)
	if err != nil {
		s.mux.Unlock()
		return 0, err
	}
	s.updateProdSeqNum(seqNum)
	return seqNum, nil
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

func (s *BufferedSinkStream) FlushNoLock(ctx context.Context, producerId commtypes.ProducerId) error {
	if len(s.sinkBuffer) != 0 {
		s.bufferEntryStats.AddSample(len(s.sinkBuffer))
		s.bufferSizeStats.AddSample(s.currentSize)
		payloadArr := commtypes.PayloadArr{
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
	if len(s.sinkBuffer) != 0 {
		s.bufferEntryStats.AddSample(len(s.sinkBuffer))
		s.bufferSizeStats.AddSample(s.currentSize)
		payloadArr := commtypes.PayloadArr{
			Payloads: s.sinkBuffer,
		}
		payloads, err := s.payloadArrSerde.Encode(payloadArr)
		if err != nil {
			s.mux.Unlock()
			return err
		}
		seqNum, err := s.Stream.Push(ctx, payloads, s.parNum, StreamEntryMeta(false, true), producerId)
		if err != nil {
			s.mux.Unlock()
			return err
		}
		s.updateProdSeqNum(seqNum)
		s.sinkBuffer = make([][]byte, 0, SINK_BUFFER_MAX_ENTRY)
		s.currentSize = 0
		s.mux.Unlock()
	} else {
		s.mux.Unlock()
	}
	debug.Assert(len(s.sinkBuffer) == 0 && s.currentSize == 0,
		"sink buffer should be empty after flush")
	return nil
}
