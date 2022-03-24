package processor

import (
	"context"
	"os"
	"sharedlog-stream/pkg/debug"
	"sharedlog-stream/pkg/errors"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"sharedlog-stream/pkg/stream/processor/store"
	"time"
)

type MeteredSource struct {
	src       Source
	latencies []int
	count     uint64
}

var _ = Source(&MeteredSource{})

func NewMeteredSource(src Source) *MeteredSource {
	return &MeteredSource{
		src:       src,
		latencies: make([]int, 0, 128),
	}
}

func (s *MeteredSource) Consume(ctx context.Context, parNum uint8) ([]commtypes.MsgAndSeq, error) {
	measure_proc := os.Getenv("MEASURE_PROC")
	if measure_proc == "true" || measure_proc == "1" {
		procStart := time.Now()
		msgs, err := s.src.Consume(ctx, parNum)
		elapsed := time.Since(procStart)
		if err == errors.ErrStreamSourceTimeout {
			return msgs, err
		}
		s.latencies = append(s.latencies, int(elapsed.Microseconds()))
		s.count += uint64(len(msgs))
		debug.Fprintf(os.Stderr, "%s consumed %d\n", s.src.TopicName(), s.count)
		return msgs, err
	} else {
		return s.src.Consume(ctx, parNum)
	}
}

func (s *MeteredSource) GetLatency() []int {
	return s.latencies
}

func (s *MeteredSource) GetCount() uint64 {
	return s.count
}

func (s *MeteredSource) SetCursor(cursor uint64, parNum uint8) {
	s.src.SetCursor(cursor, parNum)
}

func (s *MeteredSource) TopicName() string {
	return s.src.TopicName()
}

func (s *MeteredSource) Stream() store.Stream {
	return s.src.Stream()
}
