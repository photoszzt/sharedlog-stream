package processor

import (
	"context"
	"os"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"time"
)

type MeteredSink struct {
	sink      Sink
	latencies []int
}

var _ = Sink(&MeteredSink{})

func NewMeteredSink(sink Sink) *MeteredSink {
	return &MeteredSink{
		sink:      sink,
		latencies: make([]int, 0, 128),
	}
}

func (s *MeteredSink) Sink(ctx context.Context, msg commtypes.Message, parNum uint8, isControl bool) error {
	measure_proc := os.Getenv("MEASURE_PROC")
	if measure_proc == "true" || measure_proc == "1" {
		procStart := time.Now()
		err := s.sink.Sink(ctx, msg, parNum, isControl)
		elapsed := time.Since(procStart)
		s.latencies = append(s.latencies, int(elapsed.Microseconds()))
		return err
	} else {
		return s.sink.Sink(ctx, msg, parNum, isControl)
	}
}

func (s *MeteredSink) GetLatency() []int {
	return s.latencies
}
