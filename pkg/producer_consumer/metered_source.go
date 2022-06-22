package producer_consumer

import (
	"context"
	"fmt"
	"os"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/debug"
	"sharedlog-stream/pkg/stats"
	"time"
)

type MeteredConsumer struct {
	ShardedSharedLogStreamConsumer
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
		ShardedSharedLogStreamConsumer: *src,
		latencies:                      stats.NewInt64Collector(src_name, stats.DEFAULT_COLLECT_DURATION),
		pToCLat:                        stats.NewInt64Collector("procTo"+src_name, stats.DEFAULT_COLLECT_DURATION),
		consumeTp:                      stats.NewThroughputCounter(src_name, stats.DEFAULT_COLLECT_DURATION),
		measure:                        checkMeasureSource(),
	}
}

func (s *MeteredConsumer) StartWarmup() {
}

func (s *MeteredConsumer) Consume(ctx context.Context, parNum uint8) (*commtypes.MsgAndSeqs, error) {
	procStart := stats.TimerBegin()
	msgs, err := s.ShardedSharedLogStreamConsumer.Consume(ctx, parNum)
	elapsed := stats.Elapsed(procStart).Microseconds()
	if err != nil {
		debug.Fprintf(os.Stderr, "[ERROR] src out err: %v\n", err)
		return msgs, err
	}
	s.latencies.AddSample(elapsed)
	s.consumeTp.Tick(uint64(msgs.TotalLen))
	err = extractProduceToConsumeTime(msgs, s.IsInitialSource(), &s.pToCLat)
	// debug.Fprintf(os.Stderr, "%s consumed %d\n", s.src.TopicName(), s.count)
	return msgs, err
}

func (s *MeteredConsumer) GetCount() uint64 {
	return s.consumeTp.GetCount()
}

func (s *MeteredConsumer) InnerSource() Consumer {
	return &s.ShardedSharedLogStreamConsumer
}
