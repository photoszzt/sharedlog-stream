package processor

import (
	"os"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"time"
)

type MeteredSource struct {
	src       Source
	latencies []int
}

func NewMeteredSource(src Source) *MeteredSource {
	return &MeteredSource{
		src:       src,
		latencies: make([]int, 0, 128),
	}
}

func (s *MeteredSource) Consume(parNum uint8) (commtypes.Message, error) {
	measure_proc := os.Getenv("MEASURE_PROC")
	if measure_proc == "true" || measure_proc == "1" {
		procStart := time.Now()
		msg, err := s.src.Consume(parNum)
		elapsed := time.Since(procStart)
		s.latencies = append(s.latencies, int(elapsed.Microseconds()))
		return msg, err
	} else {
		return s.src.Consume(parNum)
	}
}

func (s *MeteredSource) GetLatency() []int {
	return s.latencies
}
