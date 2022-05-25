package source_sink

import (
	"context"
	"fmt"
	"os"
	"sharedlog-stream/pkg/debug"
	"sharedlog-stream/pkg/stats"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"time"
)

type MeteredSource struct {
	ShardedSharedLogStreamSource
	latencies stats.Int64Collector
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

func NewMeteredSource(src *ShardedSharedLogStreamSource, warmup time.Duration) *MeteredSource {
	src_name := fmt.Sprintf("%s_src", src.TopicName())
	return &MeteredSource{
		ShardedSharedLogStreamSource: *src,
		latencies:                    stats.NewIntCollector(src_name, stats.DEFAULT_COLLECT_DURATION),
		consumeTp:                    stats.NewThroughputCounter(src_name, stats.DEFAULT_COLLECT_DURATION),
		measure:                      checkMeasureSource(),
	}
}

func (s *MeteredSource) StartWarmup() {
}

func (s *MeteredSource) Consume(ctx context.Context, parNum uint8) (*commtypes.MsgAndSeqs, error) {
	procStart := stats.TimerBegin()
	msgs, err := s.ShardedSharedLogStreamSource.Consume(ctx, parNum)
	elapsed := stats.Elapsed(procStart).Microseconds()
	if err != nil {
		debug.Fprintf(os.Stderr, "[ERROR] src out err: %v\n", err)
		return msgs, err
	}
	s.latencies.AddSample(elapsed)
	s.consumeTp.Tick(uint64(msgs.TotalLen))
	// debug.Fprintf(os.Stderr, "%s consumed %d\n", s.src.TopicName(), s.count)
	return msgs, err
}

func (s *MeteredSource) GetCount() uint64 {
	return s.consumeTp.GetCount()
}

func (s *MeteredSource) InnerSource() Source {
	return &s.ShardedSharedLogStreamSource
}
