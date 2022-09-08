package producer_consumer

import (
	"context"
	"fmt"
	"os"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/exactly_once_intr"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/stats"
	"time"
)

type MeteredConsumer struct {
	consumer  *ShardedSharedLogStreamConsumer
	latencies stats.StatsCollector[int64]
	pToCLat   stats.StatsCollector[int64]
	consumeTp stats.ThroughputCounter
	ctrlCount uint32

	measure bool
}

func checkMeasureSource() bool {
	measure_str := os.Getenv("MEASURE_SRC")
	measure := false
	if measure_str == "true" || measure_str == "1" {
		measure = true
	}
	return measure
}

var _ = MeteredConsumerIntr(&MeteredConsumer{})

func NewMeteredConsumer(src *ShardedSharedLogStreamConsumer, warmup time.Duration) *MeteredConsumer {
	src_name := fmt.Sprintf("%s_src", src.TopicName())
	return &MeteredConsumer{
		consumer:  src,
		latencies: stats.NewStatsCollector[int64](src_name, stats.DEFAULT_COLLECT_DURATION),
		pToCLat:   stats.NewStatsCollector[int64]("procTo"+src_name, stats.DEFAULT_COLLECT_DURATION),
		consumeTp: stats.NewThroughputCounter(src_name, stats.DEFAULT_COLLECT_DURATION),
		measure:   checkMeasureSource(),
		ctrlCount: 0,
	}
}

func (s *MeteredConsumer) StartWarmup() {
}

func (s *MeteredConsumer) NumSubstreamProducer() uint8 {
	return s.consumer.numSrcProducer
}

func (s *MeteredConsumer) SrcProducerEnd(prodIdx uint8) {
	s.consumer.SrcProducerEnd(prodIdx)
}

func (s *MeteredConsumer) AllProducerEnded() bool {
	return s.consumer.AllProducerEnded()
}
func (s *MeteredConsumer) SrcProducerGotScaleFence(prodIdx uint8) {
	s.consumer.SrcProducerGotScaleFence(prodIdx)
}
func (s *MeteredConsumer) AllProducerScaleFenced() bool {
	return s.consumer.AllProducerScaleFenced()
}

func (s *MeteredConsumer) RecordCurrentConsumedSeqNum(seqNum uint64) {
	s.consumer.RecordCurrentConsumedSeqNum(seqNum)
}

func (s *MeteredConsumer) CurrentConsumedSeqNum() uint64 {
	return s.consumer.CurrentConsumedSeqNum()
}

func (s *MeteredConsumer) Consume(ctx context.Context, parNum uint8) (commtypes.RawMsgAndSeq, error) {
	procStart := stats.TimerBegin()
	rawMsgSeq, err := s.consumer.Consume(ctx, parNum)
	elapsed := stats.Elapsed(procStart).Microseconds()
	if err != nil {
		// debug.Fprintf(os.Stderr, "[ERROR] src out err: %v\n", err)
		return rawMsgSeq, err
	}
	if rawMsgSeq.IsControl {
		s.ctrlCount += 1
	}
	s.latencies.AddSample(elapsed)
	if rawMsgSeq.PayloadArr != nil {
		s.consumeTp.Tick(uint64(len(rawMsgSeq.PayloadArr)))
	} else {
		s.consumeTp.Tick(1)
	}
	// debug.Fprintf(os.Stderr, "%s consumed %d\n", s.TopicName(), s.consumeTp.GetCount())
	return rawMsgSeq, err
}

func ExtractProduceToConsumeTime(s *MeteredConsumer, msg *commtypes.Message) {
	extractProduceToConsumeTime(msg, s.IsInitialSource(), &s.pToCLat)
}

func ExtractProduceToConsumeTimeMsgG[K, V any](s *MeteredConsumer, msg *commtypes.MessageG[K, V]) {
	extractProduceToConsumeTimeMsgG(msg, s.IsInitialSource(), &s.pToCLat)
}

func (s *MeteredConsumer) GetCount() uint64 {
	return s.consumeTp.GetCount()
}

func (s *MeteredConsumer) NumCtrlMsg() uint32 {
	return s.ctrlCount
}

func (s *MeteredConsumer) InnerSource() Consumer {
	return s.consumer
}

func (s *MeteredConsumer) SetCursor(cursor uint64, parNum uint8) {
	s.consumer.SetCursor(cursor, parNum)
}
func (s *MeteredConsumer) TopicName() string               { return s.consumer.TopicName() }
func (s *MeteredConsumer) Name() string                    { return s.consumer.Name() }
func (s *MeteredConsumer) SetName(name string)             { s.consumer.SetName(name) }
func (s *MeteredConsumer) Stream() sharedlog_stream.Stream { return s.consumer.Stream() }
func (s *MeteredConsumer) ConfigExactlyOnce(guarantee exactly_once_intr.GuaranteeMth) error {
	return s.consumer.ConfigExactlyOnce(guarantee)
}
func (s *MeteredConsumer) SetInitialSource(initial bool) { s.consumer.SetInitialSource(initial) }
func (s *MeteredConsumer) IsInitialSource() bool         { return s.consumer.IsInitialSource() }
