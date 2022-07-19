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
	latencies stats.Int64Collector
	pToCLat   stats.Int64Collector
	consumeTp stats.ThroughputCounter

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
		latencies: stats.NewInt64Collector(src_name, stats.DEFAULT_COLLECT_DURATION),
		pToCLat:   stats.NewInt64Collector("procTo"+src_name, stats.DEFAULT_COLLECT_DURATION),
		consumeTp: stats.NewThroughputCounter(src_name, stats.DEFAULT_COLLECT_DURATION),
		measure:   checkMeasureSource(),
	}
}

func (s *MeteredConsumer) StartWarmup() {
}

func (s *MeteredConsumer) RecordCurrentConsumedSeqNum(seqNum uint64) {
	s.consumer.RecordCurrentConsumedSeqNum(seqNum)
}

func (s *MeteredConsumer) CurrentConsumedSeqNum() uint64 {
	return s.consumer.CurrentConsumedSeqNum()
}

func (s *MeteredConsumer) Consume(ctx context.Context, parNum uint8) (*commtypes.MsgAndSeqs, error) {
	procStart := stats.TimerBegin()
	msgs, err := s.consumer.Consume(ctx, parNum)
	elapsed := stats.Elapsed(procStart).Microseconds()
	if err != nil {
		// debug.Fprintf(os.Stderr, "[ERROR] src out err: %v\n", err)
		return msgs, err
	}
	s.latencies.AddSample(elapsed)
	s.consumeTp.Tick(uint64(msgs.TotalLen))
	// debug.Fprintf(os.Stderr, "%s consumed %d\n", s.TopicName(), s.consumeTp.GetCount())
	return msgs, err
}

func (s *MeteredConsumer) ExtractProduceToConsumeTime(msg *commtypes.Message) {
	extractProduceToConsumeTime(msg, s.IsInitialSource(), &s.pToCLat)
}

func (s *MeteredConsumer) GetCount() uint64 {
	return s.consumeTp.GetCount()
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
func (s *MeteredConsumer) ConfigExactlyOnce(serdeFormat commtypes.SerdeFormat, guarantee exactly_once_intr.GuaranteeMth) error {
	return s.consumer.ConfigExactlyOnce(serdeFormat, guarantee)
}
func (s *MeteredConsumer) SetInitialSource(initial bool)    { s.consumer.SetInitialSource(initial) }
func (s *MeteredConsumer) IsInitialSource() bool            { return s.consumer.IsInitialSource() }
func (s *MeteredConsumer) MsgSerde() commtypes.MessageSerde { return s.consumer.MsgSerde() }

// func (s *MeteredConsumer) Lock() {
// 	// debug.Fprintf(os.Stderr, "lock consumer %s\n", s.Name())
// 	s.consumer.Lock()
// }
// func (s *MeteredConsumer) Unlock() {
// 	// debug.Fprintf(os.Stderr, "unlock consumer %s\n", s.Name())
// 	s.consumer.Unlock()
// }
