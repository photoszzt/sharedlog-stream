package producer_consumer

import (
	"context"
	"fmt"
	"os"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/stats"
	"sync"
	"time"
)

func checkMeasureSink() bool {
	measure_str := os.Getenv("MEASURE_SINK")
	measure := false
	if measure_str == "true" || measure_str == "1" {
		measure = true
	}
	return measure
}

type ConcurrentMeteredSink struct {
	mu                 sync.Mutex
	eventTimeLatencies []int

	ShardedSharedLogStreamProducer

	produceTp stats.ConcurrentThroughputCounter
	lat       stats.ConcurrentInt64Collector
	warmup    stats.WarmupGoroutineSafe

	measure       bool
	isFinalOutput bool
}

var _ = MeteredProducerIntr(&ConcurrentMeteredSink{})

func NewConcurrentMeteredSyncProducer(sink *ShardedSharedLogStreamProducer, warmup time.Duration) *ConcurrentMeteredSink {
	sink_name := fmt.Sprintf("%s_sink", sink.TopicName())
	return &ConcurrentMeteredSink{
		ShardedSharedLogStreamProducer: *sink,
		lat: stats.NewConcurrentInt64Collector(sink_name,
			stats.DEFAULT_COLLECT_DURATION),
		produceTp: stats.NewConcurrentThroughputCounter(sink_name,
			stats.DEFAULT_COLLECT_DURATION),
		measure:            checkMeasureSink(),
		isFinalOutput:      false,
		warmup:             stats.NewWarmupGoroutineSafeChecker(warmup),
		eventTimeLatencies: make([]int, 0),
	}
}

func (s *ConcurrentMeteredSink) MarkFinalOutput() {
	s.isFinalOutput = true
}

func (s *ConcurrentMeteredSink) StartWarmup() {
	if s.measure {
		s.warmup.StartWarmup()
	}
}

func (s *ConcurrentMeteredSink) GetEventTimeLatency() []int {
	return s.eventTimeLatencies
}

func (s *ConcurrentMeteredSink) GetCount() uint64 {
	return s.produceTp.GetCount()
}

func (s *ConcurrentMeteredSink) InitFlushTimer() {}

func (s *ConcurrentMeteredSink) Produce(ctx context.Context, msg commtypes.Message,
	parNum uint8, isControl bool,
) error {
	err := assignInjTime(&msg)
	if err != nil {
		return err
	}
	s.produceTp.Tick(1)
	if s.measure {
		s.warmup.Check()
		if s.warmup.AfterWarmup() {
			procStart := time.Now()
			if s.isFinalOutput {
				ts, err := extractEventTs(&msg)
				if err != nil {
					return err
				}
				if ts != 0 {
					els := int(procStart.UnixMilli() - ts)
					s.mu.Lock()
					s.eventTimeLatencies = append(s.eventTimeLatencies, els)
					s.mu.Unlock()
				}
			}
		}
	}
	procStart := stats.TimerBegin()
	err = s.ShardedSharedLogStreamProducer.Produce(ctx, msg, parNum, isControl)
	elapsed := stats.Elapsed(procStart).Microseconds()
	s.lat.AddSample(elapsed)
	return err
}

type MeteredProducer struct {
	ShardedSharedLogStreamProducer
	eventTimeLatencies []int
	latencies          stats.Int64Collector
	produceTp          stats.ThroughputCounter
	warmup             stats.Warmup
	measure            bool
	isFinalOutput      bool
}

func NewMeteredProducer(sink *ShardedSharedLogStreamProducer, warmup time.Duration) *MeteredProducer {
	sink_name := fmt.Sprintf("%s_sink", sink.TopicName())
	return &MeteredProducer{
		ShardedSharedLogStreamProducer: *sink,
		latencies:                      stats.NewInt64Collector(sink_name, stats.DEFAULT_COLLECT_DURATION),
		produceTp:                      stats.NewThroughputCounter(sink_name, stats.DEFAULT_COLLECT_DURATION),
		measure:                        checkMeasureSink(),
		isFinalOutput:                  false,
		warmup:                         stats.NewWarmupChecker(warmup),
	}
}

func (s *MeteredProducer) InitFlushTimer() {}

func (s *MeteredProducer) MarkFinalOutput() {
	s.isFinalOutput = true
}

func (s *MeteredProducer) StartWarmup() {
	if s.measure {
		s.warmup.StartWarmup()
	}
}

func (s *MeteredProducer) Produce(ctx context.Context, msg commtypes.Message, parNum uint8, isControl bool) error {
	err := assignInjTime(&msg)
	if err != nil {
		return err
	}
	s.produceTp.Tick(1)
	if s.measure {
		s.warmup.Check()
		if s.warmup.AfterWarmup() {
			procStart := time.Now()
			if s.isFinalOutput {
				ts, err := extractEventTs(&msg)
				if err != nil {
					return err
				}
				if ts != 0 {
					els := int(procStart.UnixMilli() - ts)
					s.eventTimeLatencies = append(s.eventTimeLatencies, els)
				}
			}
		}
	}
	procStart := stats.TimerBegin()
	err = s.ShardedSharedLogStreamProducer.Produce(ctx, msg, parNum, isControl)
	elapsed := stats.Elapsed(procStart).Microseconds()
	s.latencies.AddSample(elapsed)
	return err
}

func extractEventTs(msg *commtypes.Message) (int64, error) {
	ts := msg.Timestamp
	if ts == 0 {
		et, ok := msg.Value.(commtypes.EventTimeExtractor)
		if ok {
			extractTs, err := et.ExtractEventTime()
			if err != nil {
				return 0, err
			}
			ts = extractTs
		}
	}
	return ts, nil
}

func (s *MeteredProducer) GetEventTimeLatency() []int {
	return s.eventTimeLatencies
}

func (s *MeteredProducer) GetCount() uint64 {
	return s.produceTp.GetCount()
}
