package sharedlog_stream

import (
	"context"
	"fmt"
	"os"
	"sharedlog-stream/pkg/debug"
	"sharedlog-stream/pkg/stream/processor"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"sync"
	"sync/atomic"
	"time"
)

type ConcurrentMeteredSink struct {
	sink *ShardedSharedLogStreamSink

	latMu     sync.Mutex
	latencies []int

	eventTimeLatencies []int

	warmup  time.Duration
	initial time.Time

	measure       bool
	isFinalOutput bool
	afterWarmup   uint32
}

var _ = processor.Sink(&ConcurrentMeteredSink{})

func NewConcurrentMeteredSink(sink *ShardedSharedLogStreamSink, warmup time.Duration) *ConcurrentMeteredSink {
	measure_str := os.Getenv("MEASURE_SINK")
	measure := false
	if measure_str == "true" || measure_str == "1" {
		measure = true
	}
	return &ConcurrentMeteredSink{
		sink:          sink,
		latencies:     make([]int, 0, 128),
		measure:       measure,
		isFinalOutput: false,
		warmup:        warmup,
		afterWarmup:   0,
	}
}

func (s *ConcurrentMeteredSink) MarkFinalOutput() {
	s.isFinalOutput = true
}

func (s *ConcurrentMeteredSink) StartWarmup() {
	if s.measure {
		s.initial = time.Now()
	}
}

func (s *ConcurrentMeteredSink) Sink(ctx context.Context, msg commtypes.Message, parNum uint8, isControl bool) error {
	debug.Assert(!s.measure || (s.warmup == 0 || (s.warmup > 0 && !s.initial.IsZero())), "warmup should initialize initial")
	if s.measure {
		if atomic.LoadUint32(&s.afterWarmup) == 0 && (s.warmup == 0 || (s.warmup > 0 && time.Since(s.initial) >= s.warmup)) {
			atomic.StoreUint32(&s.afterWarmup, 1)
		}
		if atomic.LoadUint32(&s.afterWarmup) == 1 {
			procStart := time.Now()
			if s.isFinalOutput {
				debug.Assert(msg.Timestamp != 0, "sink event ts should be set")
				els := int(procStart.UnixMilli() - msg.Timestamp)
				s.latMu.Lock()
				s.eventTimeLatencies = append(s.eventTimeLatencies, els)
				s.latMu.Unlock()
			}
			err := s.sink.Sink(ctx, msg, parNum, isControl)
			elapsed := time.Since(procStart)
			s.latMu.Lock()
			s.latencies = append(s.latencies, int(elapsed.Microseconds()))
			s.latMu.Unlock()
			return err
		}
	}
	return s.sink.Sink(ctx, msg, parNum, isControl)
}

func (s *ConcurrentMeteredSink) Flush(ctx context.Context) error {
	if s.measure {
		procStart := time.Now()
		err := s.sink.Flush(ctx)
		elapsed := time.Since(procStart)
		s.latMu.Lock()
		s.latencies = append(s.latencies, int(elapsed.Microseconds()))
		s.latMu.Unlock()
		return err
	}
	return s.sink.Flush(ctx)
}

func (s *ConcurrentMeteredSink) GetLatency() []int {
	s.latMu.Lock()
	defer s.latMu.Unlock()
	return s.latencies
}

func (s *ConcurrentMeteredSink) GetEventTimeLatency() []int {
	s.latMu.Lock()
	defer s.latMu.Unlock()
	return s.eventTimeLatencies
}

func (s *ConcurrentMeteredSink) TopicName() string {
	return s.sink.TopicName()
}

func (s *ConcurrentMeteredSink) KeySerde() commtypes.Serde {
	return s.sink.KeySerde()
}

func (s *ConcurrentMeteredSink) InitFlushTimer() {
	s.sink.InitFlushTimer()
}

func (s *ConcurrentMeteredSink) CloseAsyncPush() {
	s.sink.CloseAsyncPush()
}

func (s *ConcurrentMeteredSink) InnerSink() *ShardedSharedLogStreamSink {
	return s.sink
}

type ConcurrentMeteredSyncSink struct {
	sink *ShardedSharedLogStreamSyncSink

	latMu     sync.Mutex
	latencies []int

	eventTimeLatencies []int

	warmup  time.Duration
	initial time.Time

	measure       bool
	isFinalOutput bool
	afterWarmup   uint32
}

var _ = processor.Sink(&ConcurrentMeteredSink{})

func NewConcurrentMeteredSyncSink(sink *ShardedSharedLogStreamSyncSink, warmup time.Duration) *ConcurrentMeteredSyncSink {
	measure_str := os.Getenv("MEASURE_SINK")
	measure := false
	if measure_str == "true" || measure_str == "1" {
		measure = true
	}
	return &ConcurrentMeteredSyncSink{
		sink:          sink,
		latencies:     make([]int, 0, 128),
		measure:       measure,
		isFinalOutput: false,
		warmup:        warmup,
		afterWarmup:   0,
	}
}

func (s *ConcurrentMeteredSyncSink) MarkFinalOutput() {
	s.isFinalOutput = true
}

func (s *ConcurrentMeteredSyncSink) StartWarmup() {
	if s.measure {
		s.initial = time.Now()
	}
}

func (s *ConcurrentMeteredSyncSink) Flush(ctx context.Context) error {
	if s.measure {
		procStart := time.Now()
		err := s.sink.Flush(ctx)
		elapsed := time.Since(procStart)
		s.latMu.Lock()
		s.latencies = append(s.latencies, int(elapsed.Microseconds()))
		s.latMu.Unlock()
		return err
	}
	return s.sink.Flush(ctx)
}

func (s *ConcurrentMeteredSyncSink) KeySerde() commtypes.Serde {
	return s.sink.KeySerde()
}

func (s *ConcurrentMeteredSyncSink) TopicName() string {
	return s.sink.TopicName()
}

func (s *ConcurrentMeteredSyncSink) GetLatency() []int {
	return s.latencies
}

func (s *ConcurrentMeteredSyncSink) GetEventTimeLatency() []int {
	return s.eventTimeLatencies
}

func (s *ConcurrentMeteredSyncSink) Sink(ctx context.Context, msg commtypes.Message, parNum uint8, isControl bool) error {
	debug.Assert(!s.measure || (s.warmup == 0 || (s.warmup > 0 && !s.initial.IsZero())), "warmup should initialize initial")
	if s.measure {
		if atomic.LoadUint32(&s.afterWarmup) == 0 && (s.warmup == 0 || (s.warmup > 0 && time.Since(s.initial) >= s.warmup)) {
			atomic.StoreUint32(&s.afterWarmup, 1)
		}
		if atomic.LoadUint32(&s.afterWarmup) == 1 {
			procStart := time.Now()
			if s.isFinalOutput {
				debug.Assert(msg.Timestamp != 0, "sink event ts should be set")
				els := int(procStart.UnixMilli() - msg.Timestamp)
				s.latMu.Lock()
				s.eventTimeLatencies = append(s.eventTimeLatencies, els)
				s.latMu.Unlock()
			}
			err := s.sink.Sink(ctx, msg, parNum, isControl)
			elapsed := time.Since(procStart)
			s.latMu.Lock()
			s.latencies = append(s.latencies, int(elapsed.Microseconds()))
			s.latMu.Unlock()
			return err
		}
	}
	return s.sink.Sink(ctx, msg, parNum, isControl)
}

type MeteredSink struct {
	sink *ShardedSharedLogStreamSink

	latencies          []int
	eventTimeLatencies []int

	warmup  time.Duration
	initial time.Time

	measure       bool
	isFinalOutput bool
	afterWarmup   bool
}

func NewMeteredSink(sink *ShardedSharedLogStreamSink, warmup time.Duration) *MeteredSink {
	measure_str := os.Getenv("MEASURE_SINK")
	measure := false
	if measure_str == "true" || measure_str == "1" {
		measure = true
	}
	return &MeteredSink{
		sink:          sink,
		latencies:     make([]int, 0, 128),
		measure:       measure,
		isFinalOutput: false,
		warmup:        warmup,
		afterWarmup:   false,
	}
}

func (s *MeteredSink) MarkFinalOutput() {
	s.isFinalOutput = true
}

func (s *MeteredSink) StartWarmup() {
	if s.measure {
		s.initial = time.Now()
	}
}

func (s *MeteredSink) Sink(ctx context.Context, msg commtypes.Message, parNum uint8, isControl bool) error {
	debug.Assert(s.warmup == 0 || (s.warmup > 0 && !s.initial.IsZero()), "warmup should initialize initial")
	if s.measure {
		if !s.afterWarmup && (s.warmup == 0 || (s.warmup > 0 && time.Since(s.initial) >= s.warmup)) {
			s.afterWarmup = true
		}
		if s.afterWarmup {
			procStart := time.Now()
			if s.isFinalOutput {
				ts := msg.Timestamp
				if ts == 0 {
					extractTs, err := msg.Value.(commtypes.StreamTimeExtractor).ExtractStreamTime()
					if err != nil || extractTs == 0 {
						return fmt.Errorf("time stampt should not be zero")
					}
					ts = extractTs
				}
				els := int(procStart.UnixMilli() - ts)
				s.eventTimeLatencies = append(s.eventTimeLatencies, els)
			}
			err := s.sink.Sink(ctx, msg, parNum, isControl)
			elapsed := time.Since(procStart)
			s.latencies = append(s.latencies, int(elapsed.Microseconds()))
			return err
		}
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

func (s *MeteredSink) Flush(ctx context.Context) error {
	if s.measure {
		procStart := time.Now()
		err := s.sink.FlushNoLock(ctx)
		elapsed := time.Since(procStart)
		s.latencies = append(s.latencies, int(elapsed.Microseconds()))
		return err
	}
	return s.sink.Flush(ctx)
}

func (s *MeteredSink) GetEventTimeLatency() []int {
	return s.eventTimeLatencies
}

func (s *MeteredSink) InitFlushTimer() {
	s.sink.InitFlushTimer()
}

func (s *MeteredSink) CloseAsyncPush() {
	s.sink.CloseAsyncPush()
}

func (s *MeteredSink) InnerSink() *ShardedSharedLogStreamSink {
	return s.sink
}
