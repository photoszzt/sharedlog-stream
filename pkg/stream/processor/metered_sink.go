package processor

import (
	"context"
	"os"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"sync"
	"time"
)

type MeteredSink struct {
	sink Sink

	latMu     sync.Mutex
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
		s.latMu.Lock()
		s.latencies = append(s.latencies, int(elapsed.Microseconds()))
		s.latMu.Unlock()
		return err
	} else {
		return s.sink.Sink(ctx, msg, parNum, isControl)
	}
}

func (s *MeteredSink) GetLatency() []int {
	s.latMu.Lock()
	defer s.latMu.Unlock()
	return s.latencies
}

func (s *MeteredSink) TopicName() string {
	return s.sink.TopicName()
}

func (s *MeteredSink) KeySerde() commtypes.Serde {
	return s.sink.KeySerde()
}
