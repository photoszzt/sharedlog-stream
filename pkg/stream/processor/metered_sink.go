package processor

import (
	"context"
	"os"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"sync"
	"time"
)

type ConcurrentMeteredSink struct {
	sink Sink

	latMu     sync.Mutex
	latencies []int

	measure bool
}

var _ = Sink(&ConcurrentMeteredSink{})

func NewConcurrentMeteredSink(sink Sink) *ConcurrentMeteredSink {
	measure_str := os.Getenv("MEASURE_PROC")
	measure := false
	if measure_str == "true" || measure_str == "1" {
		measure = true
	}
	return &ConcurrentMeteredSink{
		sink:      sink,
		latencies: make([]int, 0, 128),
		measure:   measure,
	}
}

func (s *ConcurrentMeteredSink) Sink(ctx context.Context, msg commtypes.Message, parNum uint8, isControl bool) error {
	if s.measure {
		procStart := time.Now()
		err := s.sink.Sink(ctx, msg, parNum, isControl)
		elapsed := time.Since(procStart)
		s.latMu.Lock()
		s.latencies = append(s.latencies, int(elapsed.Microseconds()))
		s.latMu.Unlock()
		return err
	}
	return s.sink.Sink(ctx, msg, parNum, isControl)
}

func (s *ConcurrentMeteredSink) GetLatency() []int {
	s.latMu.Lock()
	defer s.latMu.Unlock()
	return s.latencies
}

func (s *ConcurrentMeteredSink) TopicName() string {
	return s.sink.TopicName()
}

func (s *ConcurrentMeteredSink) KeySerde() commtypes.Serde {
	return s.sink.KeySerde()
}

type MeteredSink struct {
	sink Sink

	latencies []int

	measure bool
}

var _ = Sink(&MeteredSink{})

func NewMeteredSink(sink Sink) *MeteredSink {
	measure_str := os.Getenv("MEASURE_PROC")
	measure := false
	if measure_str == "true" || measure_str == "1" {
		measure = true
	}
	return &MeteredSink{
		sink:      sink,
		latencies: make([]int, 0, 128),
		measure:   measure,
	}
}

func (s *MeteredSink) Sink(ctx context.Context, msg commtypes.Message, parNum uint8, isControl bool) error {
	if s.measure {
		procStart := time.Now()
		err := s.sink.Sink(ctx, msg, parNum, isControl)
		elapsed := time.Since(procStart)
		s.latencies = append(s.latencies, int(elapsed.Microseconds()))
		return err
	}
	return s.sink.Sink(ctx, msg, parNum, isControl)
}

func (s *MeteredSink) GetLatency() []int {
	return s.latencies
}

func (s *MeteredSink) TopicName() string {
	return s.sink.TopicName()
}

func (s *MeteredSink) KeySerde() commtypes.Serde {
	return s.sink.KeySerde()
}
