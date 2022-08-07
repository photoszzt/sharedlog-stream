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

type MeteredConsumer[K, V any] struct {
	consumer  *ShardedSharedLogStreamConsumer[K, V]
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

var _ = MeteredConsumerIntr(&MeteredConsumer[int, string]{})

func NewMeteredConsumer[K, V any](src *ShardedSharedLogStreamConsumer[K, V], warmup time.Duration) *MeteredConsumer[K, V] {
	src_name := fmt.Sprintf("%s_src", src.TopicName())
	return &MeteredConsumer[K, V]{
		consumer:  src,
		latencies: stats.NewStatsCollector[int64](src_name, stats.DEFAULT_COLLECT_DURATION),
		pToCLat:   stats.NewStatsCollector[int64]("procTo"+src_name, stats.DEFAULT_COLLECT_DURATION),
		consumeTp: stats.NewThroughputCounter(src_name, stats.DEFAULT_COLLECT_DURATION),
		measure:   checkMeasureSource(),
		ctrlCount: 0,
	}
}

func (s *MeteredConsumer[K, V]) StartWarmup() {
}

func (s *MeteredConsumer[K, V]) RecordCurrentConsumedSeqNum(seqNum uint64) {
	s.consumer.RecordCurrentConsumedSeqNum(seqNum)
}

func (s *MeteredConsumer[K, V]) CurrentConsumedSeqNum() uint64 {
	return s.consumer.CurrentConsumedSeqNum()
}

func (s *MeteredConsumer[K, V]) Consume(ctx context.Context, parNum uint8) (*commtypes.MsgAndSeqs, error) {
	procStart := stats.TimerBegin()
	msgs, err := s.consumer.Consume(ctx, parNum)
	elapsed := stats.Elapsed(procStart).Microseconds()
	if err != nil {
		// debug.Fprintf(os.Stderr, "[ERROR] src out err: %v\n", err)
		return msgs, err
	}
	if msgs.Msgs.IsControl {
		s.ctrlCount += 1
	}
	s.latencies.AddSample(elapsed)
	s.consumeTp.Tick(uint64(msgs.TotalLen))
	// debug.Fprintf(os.Stderr, "%s consumed %d\n", s.TopicName(), s.consumeTp.GetCount())
	return msgs, err
}

func (s *MeteredConsumer[K, V]) ExtractProduceToConsumeTime(msg *commtypes.Message) {
	extractProduceToConsumeTime(msg, s.IsInitialSource(), &s.pToCLat)
}

func (s *MeteredConsumer[K, V]) GetCount() uint64 {
	return s.consumeTp.GetCount()
}

func (s *MeteredConsumer[K, V]) NumCtrlMsg() uint32 {
	return s.ctrlCount
}

func (s *MeteredConsumer[K, V]) InnerSource() Consumer {
	return s.consumer
}

func (s *MeteredConsumer[K, V]) SetCursor(cursor uint64, parNum uint8) {
	s.consumer.SetCursor(cursor, parNum)
}
func (s *MeteredConsumer[K, V]) TopicName() string               { return s.consumer.TopicName() }
func (s *MeteredConsumer[K, V]) Name() string                    { return s.consumer.Name() }
func (s *MeteredConsumer[K, V]) SetName(name string)             { s.consumer.SetName(name) }
func (s *MeteredConsumer[K, V]) Stream() sharedlog_stream.Stream { return s.consumer.Stream() }
func (s *MeteredConsumer[K, V]) ConfigExactlyOnce(guarantee exactly_once_intr.GuaranteeMth) error {
	return s.consumer.ConfigExactlyOnce(guarantee)
}
func (s *MeteredConsumer[K, V]) SetInitialSource(initial bool) { s.consumer.SetInitialSource(initial) }
func (s *MeteredConsumer[K, V]) IsInitialSource() bool         { return s.consumer.IsInitialSource() }
func (s *MeteredConsumer[K, V]) MsgSerde() commtypes.MessageSerdeG[K, V] {
	return s.consumer.MsgSerde()
}
