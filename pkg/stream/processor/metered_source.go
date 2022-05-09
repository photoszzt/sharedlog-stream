package processor

import (
	"context"
	"os"
	"sharedlog-stream/pkg/debug"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"sharedlog-stream/pkg/stream/processor/store"
	"time"
)

type MeteredSource struct {
	src         Source
	warmup      time.Duration
	initial     time.Time
	latencies   []int
	count       uint64
	measure     bool
	afterWarmup bool
}

var _ = Source(&MeteredSource{})

func NewMeteredSource(src Source, warmup time.Duration) *MeteredSource {
	measure_str := os.Getenv("MEASURE_SRC")
	measure := false
	if measure_str == "true" || measure_str == "1" {
		measure = true
	}
	return &MeteredSource{
		src:         src,
		latencies:   make([]int, 0, 128),
		measure:     measure,
		warmup:      warmup,
		afterWarmup: false,
	}
}

func (s *MeteredSource) StartWarmup() {
	if s.measure {
		s.initial = time.Now()
	}
}

func (s *MeteredSource) Consume(ctx context.Context, parNum uint8) (*commtypes.MsgAndSeqs, error) {
	debug.Assert(!s.measure || (s.warmup == 0 || (s.warmup > 0 && !s.initial.IsZero())), "warmup should initialize initial")
	if s.measure {
		if !s.afterWarmup && (s.warmup == 0 || (s.warmup > 0 && time.Since(s.initial) >= s.warmup)) {
			s.afterWarmup = true
		}
		if s.afterWarmup {
			procStart := time.Now()
			msgs, err := s.src.Consume(ctx, parNum)
			elapsed := time.Since(procStart)
			if err != nil {
				return msgs, err
			}
			s.latencies = append(s.latencies, int(elapsed.Microseconds()))
			s.count += uint64(msgs.TotalLen)
			// debug.Fprintf(os.Stderr, "%s consumed %d\n", s.src.TopicName(), s.count)
			return msgs, err
		}
	}
	return s.src.Consume(ctx, parNum)
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

func (s *MeteredSource) InnerSource() Source {
	return s.src
}
