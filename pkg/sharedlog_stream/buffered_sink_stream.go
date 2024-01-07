package sharedlog_stream

import (
	"context"
	"fmt"
	"os"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/debug"
	"sharedlog-stream/pkg/exactly_once_intr"
	"sharedlog-stream/pkg/stats"
	"sharedlog-stream/pkg/utils/syncutils"
	"sync/atomic"
	"time"
)

const (
	SINK_BUFFER_MAX_ENTRY = 12800
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

	flushBufferStats stats.PrintLogStatsCollector[int64]
	bufferEntryStats stats.StatsCollector[int]
	bufferSizeStats  stats.StatsCollector[int]

	initialProdInEpoch   uint64
	lastMarkerSeq        uint64
	sink_buffer_max_size int
	currentSize          int
	initProdIsSet        atomic.Bool
	guarantee            exactly_once_intr.GuaranteeMth
	parNum               uint8
}

func NewBufferedSinkStream(stream *SharedLogStream, parNum uint8, bufMaxSize uint32) *BufferedSinkStream {
	fmt.Fprintf(os.Stderr, "new buffered sink stream %s[%d] with bufMaxSize %d\n", stream.topicName, parNum, bufMaxSize)
	s := &BufferedSinkStream{
		sinkBuffer:      make([][]byte, 0, SINK_BUFFER_MAX_ENTRY),
		payloadArrSerde: DEFAULT_PAYLOAD_ARR_SERDEG,
		parNum:          parNum,
		Stream:          stream,
		currentSize:     0,
		guarantee:       exactly_once_intr.AT_LEAST_ONCE,
		bufferEntryStats: stats.NewStatsCollector[int](fmt.Sprintf("%s_bufEntry_%v", stream.topicName, parNum),
			stats.DEFAULT_COLLECT_DURATION),
		bufferSizeStats: stats.NewStatsCollector[int](fmt.Sprintf("%s_bufSize_%v", stream.topicName, parNum),
			stats.DEFAULT_COLLECT_DURATION),
		flushBufferStats:     stats.NewPrintLogStatsCollector[int64](fmt.Sprintf("%s_flushBuf_%v", stream.topicName, parNum)),
		sink_buffer_max_size: int(bufMaxSize),
		lastMarkerSeq:        0,
	}
	s.initProdIsSet.Store(false)
	return s
}

func (s *BufferedSinkStream) OutputRemainingStats() {
	s.bufferEntryStats.PrintRemainingStats()
	s.bufferSizeStats.PrintRemainingStats()
	s.flushBufferStats.PrintRemainingStats()
}

func (s *BufferedSinkStream) SetLastMarkerSeq(seq uint64) {
	s.lastMarkerSeq = seq
}

// don't mix the nolock version and goroutine safe version
func (s *BufferedSinkStream) BufPushAutoFlushNoLock(ctx context.Context, payload []byte,
	producerId commtypes.ProducerId, flushCallback exactly_once_intr.FlushCallbackFunc,
) error {
	// debug.Fprintf(os.Stderr, "%s(%d) bufpush payload: %d bytes\n", s.Stream.topicName, s.parNum, len(payload))
	payload_size := len(payload)
	if len(s.sinkBuffer) < SINK_BUFFER_MAX_ENTRY && s.currentSize+payload_size < s.sink_buffer_max_size {
		s.sinkBuffer = append(s.sinkBuffer, payload)
		s.currentSize += payload_size
		return nil
	}
	s.bufferEntryStats.AddSample(len(s.sinkBuffer))
	s.bufferSizeStats.AddSample(s.currentSize)
	payloadArr := commtypes.PayloadArr{
		Payloads: s.sinkBuffer,
	}
	payloads, err := s.payloadArrSerde.Encode(payloadArr)
	if err != nil {
		return err
	}
	err = flushCallback(ctx)
	if err != nil {
		return err
	}
	tags := []uint64{NameHashWithPartition(s.Stream.topicNameHash, s.parNum)}
	seqNum, err := s.Stream.PushWithTag(ctx, payloads, s.parNum, tags,
		nil, ArrRecordMeta, producerId)
	if err != nil {
		return err
	}
	err = s.updateProdSeqNum(ctx, seqNum, s.parNum, tags[0], producerId)
	if err != nil {
		return fmt.Errorf("updateProdSeqNum(%s[%d]): %v, appended seqNum: %#x, prodId: %s, tag %#x",
			s.Stream.topicName, s.parNum, err, seqNum, producerId.String(), tags[0])
	}
	s.sinkBuffer = make([][]byte, 0, SINK_BUFFER_MAX_ENTRY)
	s.sinkBuffer = append(s.sinkBuffer, payload)
	s.currentSize = payload_size
	return nil
}

func (s *BufferedSinkStream) BufPushAutoFlushGoroutineSafe(ctx context.Context, payload []byte,
	producerId commtypes.ProducerId, flushCallback exactly_once_intr.FlushCallbackFunc,
) error {
	s.mux.Lock()
	payload_size := len(payload)
	if len(s.sinkBuffer) < SINK_BUFFER_MAX_ENTRY && s.currentSize+payload_size < s.sink_buffer_max_size {
		s.sinkBuffer = append(s.sinkBuffer, payload)
		s.currentSize += payload_size
		s.mux.Unlock()
		return nil
	}
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
	err = flushCallback(ctx)
	if err != nil {
		s.mux.Unlock()
		return err
	}
	tags := []uint64{NameHashWithPartition(s.Stream.topicNameHash, s.parNum)}
	seqNum, err := s.Stream.PushWithTag(ctx, payloads, s.parNum, tags,
		nil, ArrRecordMeta, producerId)
	if err != nil {
		s.mux.Unlock()
		return err
	}
	err = s.updateProdSeqNumLocked(ctx, seqNum, s.parNum, tags[0], producerId)
	if err != nil {
		s.mux.Unlock()
		return fmt.Errorf("updateProdSeqNum(%s[%d]): %v, appended seqNum: %#x, prodId: %s, tag %#x",
			s.Stream.topicName, s.parNum, err, seqNum, producerId.String(), tags[0])
	}
	s.sinkBuffer = make([][]byte, 0, SINK_BUFFER_MAX_ENTRY)
	s.sinkBuffer = append(s.sinkBuffer, payload)
	s.currentSize = payload_size
	s.mux.Unlock()
	return nil
}

func (s *BufferedSinkStream) updateProdSeqNumLocked(ctx context.Context, appendedSeq uint64, parNum uint8, tag uint64, prodId commtypes.ProducerId) error {
	if s.guarantee != exactly_once_intr.EPOCH_MARK {
		return nil
	}
	if s.initProdIsSet.Load() {
		return nil
	}
	r, err := s.Stream.ReadFromSeqNumWithTag(ctx, s.lastMarkerSeq+1, appendedSeq, parNum, tag, prodId)
	if err != nil {
		return fmt.Errorf("ReadFromSeqNumWithTag: %v, from: %#x",
			err, s.lastMarkerSeq+1)
	}
	s.initialProdInEpoch = r.LogSeqNum
	// debug.Fprintf(os.Stderr, "update initial prod in epoch: %#x\n", s.initialProdInEpoch)
	s.initProdIsSet.Store(true)
	return nil
}

func (s *BufferedSinkStream) updateProdSeqNum(ctx context.Context, appendedSeq uint64, parNum uint8, tag uint64, prodId commtypes.ProducerId) error {
	if s.guarantee != exactly_once_intr.EPOCH_MARK {
		return nil
	}
	if s.initProdIsSet.Load() {
		return nil
	}
	r, err := s.Stream.ReadFromSeqNumWithTag(ctx, s.lastMarkerSeq+1, appendedSeq, parNum, tag, prodId)
	if err != nil {
		return fmt.Errorf("ReadFromSeqNumWithTag: %v, from: %#x",
			err, s.lastMarkerSeq+1)
	}
	s.mux.Lock()
	s.initialProdInEpoch = r.LogSeqNum
	// debug.Fprintf(os.Stderr, "update initial prod in epoch: %#x\n", s.initialProdInEpoch)
	s.initProdIsSet.Store(true)
	s.mux.Unlock()
	return nil
}

func (s *BufferedSinkStream) Push(ctx context.Context, payload []byte, parNum uint8,
	meta LogEntryMeta, producerId commtypes.ProducerId,
) (uint64, error) {
	seqNum, err := s.Stream.Push(ctx, payload, parNum, meta, producerId)
	if err != nil {
		return 0, err
	}
	tag := NameHashWithPartition(s.Stream.topicNameHash, parNum)
	err = s.updateProdSeqNum(ctx, seqNum, parNum, tag, producerId)
	if err != nil {
		return 0, fmt.Errorf("updateProdSeqNum(%s[%d]): %v, appended seqNum: %#x, prodId: %s, tag %#x",
			s.Stream.topicName, s.parNum, err, seqNum, producerId.String(), tag)
	}
	return seqNum, nil
}

func (s *BufferedSinkStream) PushWithTag(ctx context.Context, payload []byte, parNum uint8, tags []uint64,
	additionalTopic []string, meta LogEntryMeta, producerId commtypes.ProducerId,
) (uint64, error) {
	seqNum, err := s.Stream.PushWithTag(ctx, payload, parNum, tags, additionalTopic, meta, producerId)
	if err != nil {
		return 0, err
	}

	err = s.updateProdSeqNum(ctx, seqNum, parNum, tags[0], producerId)
	if err != nil {
		return 0, fmt.Errorf("updateProdSeqNum(%s[%d]): %v, appended seqNum: %#x, prodId: %s, tag %#x",
			s.Stream.topicName, s.parNum, err, seqNum, producerId.String(), tags[0])
	}
	return seqNum, nil
}

func (s *BufferedSinkStream) ExactlyOnce(gua exactly_once_intr.GuaranteeMth) {
	s.guarantee = gua
}

func (s *BufferedSinkStream) ResetInitialProd() {
	if s.guarantee == exactly_once_intr.EPOCH_MARK {
		s.initProdIsSet.CompareAndSwap(true, false)
	}
}

func (s *BufferedSinkStream) GetInitialProdSeqNum() uint64 {
	return s.initialProdInEpoch
}

func (s *BufferedSinkStream) FlushNoLock(ctx context.Context,
	producerId commtypes.ProducerId,
) (uint32, error) {
	if len(s.sinkBuffer) == 0 {
		debug.Assert(len(s.sinkBuffer) == 0 && s.currentSize == 0,
			"sink buffer should be empty after flush")
		return 0, nil
	}
	s.bufferEntryStats.AddSample(len(s.sinkBuffer))
	s.bufferSizeStats.AddSample(s.currentSize)
	tBeg := time.Now()
	payloadArr := commtypes.PayloadArr{
		Payloads: s.sinkBuffer,
	}
	payloads, err := s.payloadArrSerde.Encode(payloadArr)
	if err != nil {
		return 0, err
	}
	if err != nil {
		return 0, err
	}
	tags := []uint64{NameHashWithPartition(s.Stream.topicNameHash, s.parNum)}
	seqNum, err := s.Stream.PushWithTag(ctx, payloads, s.parNum, tags,
		nil, StreamEntryMeta(false, true), producerId)
	if err != nil {
		return 0, err
	}
	// debug.Fprintf(os.Stderr, "FlushNoLock %s[%d] logEntry %#x prodId %s\n",
	// 	s.Stream.topicName, s.parNum, n, producerId.String())
	err = s.updateProdSeqNum(ctx, seqNum, s.parNum, tags[0], producerId)
	if err != nil {
		return 0, fmt.Errorf("updateProdSeqNum(%s[%d]): %v, appended seqNum: %#x, prodId: %s, tag %#x",
			s.Stream.topicName, s.parNum, err, seqNum, producerId.String(), tags[0])
	}
	s.sinkBuffer = make([][]byte, 0, SINK_BUFFER_MAX_ENTRY)
	s.currentSize = 0
	debug.Assert(len(s.sinkBuffer) == 0 && s.currentSize == 0,
		"sink buffer should be empty after flush")
	flushElapsed := time.Since(tBeg).Microseconds()
	s.flushBufferStats.AddSample(flushElapsed)
	return 1, nil
}

func (s *BufferedSinkStream) FlushGoroutineSafe(ctx context.Context,
	producerId commtypes.ProducerId,
) (uint32, error) {
	s.mux.Lock()
	if len(s.sinkBuffer) == 0 {
		s.mux.Unlock()
		debug.Assert(s.currentSize == 0,
			"sink buffer should be empty after flush")
		return 0, nil
	}
	s.bufferEntryStats.AddSample(len(s.sinkBuffer))
	s.bufferSizeStats.AddSample(s.currentSize)
	tBeg := time.Now()
	payloadArr := commtypes.PayloadArr{
		Payloads: s.sinkBuffer,
	}
	payloads, err := s.payloadArrSerde.Encode(payloadArr)
	if err != nil {
		s.mux.Unlock()
		return 0, err
	}
	tags := []uint64{NameHashWithPartition(s.Stream.topicNameHash, s.parNum)}
	if err != nil {
		s.mux.Unlock()
		return 0, err
	}
	seqNum, err := s.Stream.PushWithTag(ctx, payloads, s.parNum,
		tags, nil, StreamEntryMeta(false, true), producerId)
	if err != nil {
		s.mux.Unlock()
		return 0, err
	}
	err = s.updateProdSeqNumLocked(ctx, seqNum, s.parNum, tags[0], producerId)
	if err != nil {
		s.mux.Unlock()
		return 0, fmt.Errorf("updateProdSeqNum(%s[%d]): %v, appended seqNum: %#x, prodId: %s, tag %#x",
			s.Stream.topicName, s.parNum, err, seqNum, producerId.String(), tags[0])
	}
	s.sinkBuffer = make([][]byte, 0, SINK_BUFFER_MAX_ENTRY)
	s.currentSize = 0
	debug.Assert(len(s.sinkBuffer) == 0 && s.currentSize == 0,
		"sink buffer should be empty after flush")
	flushElapsed := time.Since(tBeg).Microseconds()
	s.flushBufferStats.AddSample(flushElapsed)
	s.mux.Unlock()
	return 1, nil
}
