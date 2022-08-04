package producer_consumer

import (
	"context"
	"fmt"
	"os"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/exactly_once_intr"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/stats"
	"sync/atomic"
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

type ConcurrentMeteredSink[K, V any] struct {
	// mu syncutils.Mutex
	// eventTimeLatencies []int

	producer *ShardedSharedLogStreamProducer[K, V]

	produceTp       *stats.ConcurrentThroughputCounter
	lat             *stats.ConcurrentStatsCollector[int64]
	eventTimeSample *stats.ConcurrentStatsCollector[int64]
	ctrlCount       uint64
	warmup          stats.WarmupGoroutineSafe

	measure bool
}

var _ = MeteredProducerIntr(&ConcurrentMeteredSink[int, string]{})

func NewConcurrentMeteredSyncProducer[K, V any](sink *ShardedSharedLogStreamProducer[K, V], warmup time.Duration) *ConcurrentMeteredSink[K, V] {
	sink_name := fmt.Sprintf("%s_sink", sink.TopicName())
	return &ConcurrentMeteredSink[K, V]{
		producer: sink,
		lat: stats.NewConcurrentStatsCollector[int64](sink_name,
			stats.DEFAULT_COLLECT_DURATION),
		produceTp: stats.NewConcurrentThroughputCounter(sink_name,
			stats.DEFAULT_COLLECT_DURATION),
		measure: checkMeasureSink(),
		warmup:  stats.NewWarmupGoroutineSafeChecker(warmup),
		// eventTimeLatencies: make([]int, 0),
		eventTimeSample: stats.NewConcurrentStatsCollector[int64](sink_name+"_ets",
			stats.DEFAULT_COLLECT_DURATION),
	}
}

func (s *ConcurrentMeteredSink[K, V]) MarkFinalOutput() {
	s.producer.MarkFinalOutput()
}

func (s *ConcurrentMeteredSink[K, V]) IsFinalOutput() bool {
	return s.producer.IsFinalOutput()
}

func (s *ConcurrentMeteredSink[K, V]) StartWarmup() {
	if s.measure {
		s.warmup.StartWarmup()
	}
}

// func (s *ConcurrentMeteredSink[K, V]) GetEventTimeLatency() []int {
// 	return s.eventTimeLatencies
// }

func (s *ConcurrentMeteredSink[K, V]) GetCount() uint64 {
	return s.produceTp.GetCount()
}

func (s *ConcurrentMeteredSink[K, V]) InitFlushTimer() {}

func (s *ConcurrentMeteredSink[K, V]) Produce(ctx context.Context, msg commtypes.Message,
	parNum uint8, isControl bool,
) error {
	if s.IsFinalOutput() {
		procStart := time.Now()
		ts, err := extractEventTs(&msg)
		if err != nil {
			return err
		}
		if ts != 0 {
			els := procStart.UnixMilli() - ts
			// s.mu.Lock()
			// s.eventTimeLatencies = append(s.eventTimeLatencies, els)
			// s.mu.Unlock()
			s.eventTimeSample.AddSample(els)
		}
	}
	assignInjTime(&msg)
	s.produceTp.Tick(1)
	if isControl {
		atomic.AddUint64(&s.ctrlCount, 1)
	}
	// if s.measure {
	// 	s.warmup.Check()
	// 	if s.warmup.AfterWarmup() {
	// 		if s.isFinalOutput {
	// 			procStart := time.Now()
	// 			ts, err := extractEventTs(&msg)
	// 			if err != nil {
	// 				return err
	// 			}
	// 			if ts != 0 {
	// 				els := int(procStart.UnixMilli() - ts)
	// 				s.mu.Lock()
	// 				s.eventTimeLatencies = append(s.eventTimeLatencies, els)
	// 				s.mu.Unlock()
	// 			}
	// 		}
	// 	}
	// }
	procStart := stats.TimerBegin()
	err := s.producer.Produce(ctx, msg, parNum, isControl)
	elapsed := stats.Elapsed(procStart).Microseconds()
	s.lat.AddSample(elapsed)
	return err
}

func (s *ConcurrentMeteredSink[K, V]) TopicName() string               { return s.producer.TopicName() }
func (s *ConcurrentMeteredSink[K, V]) Name() string                    { return s.producer.Name() }
func (s *ConcurrentMeteredSink[K, V]) SetName(name string)             { s.producer.SetName(name) }
func (s *ConcurrentMeteredSink[K, V]) KeyEncoder() commtypes.Encoder   { return s.producer.KeyEncoder() }
func (s *ConcurrentMeteredSink[K, V]) Flush(ctx context.Context) error { return s.producer.Flush(ctx) }
func (s *ConcurrentMeteredSink[K, V]) ConfigExactlyOnce(rem exactly_once_intr.ReadOnlyExactlyOnceManager,
	guarantee exactly_once_intr.GuaranteeMth,
) {
	s.producer.ConfigExactlyOnce(rem, guarantee)
}
func (s *ConcurrentMeteredSink[K, V]) Stream() sharedlog_stream.Stream {
	return s.producer.Stream()
}
func (s *ConcurrentMeteredSink[K, V]) GetInitialProdSeqNum(substreamNum uint8) uint64 {
	return s.producer.GetInitialProdSeqNum(substreamNum)
}
func (s *ConcurrentMeteredSink[K, V]) GetCurrentProdSeqNum(substreamNum uint8) uint64 {
	return s.producer.GetCurrentProdSeqNum(substreamNum)
}
func (s *ConcurrentMeteredSink[K, V]) ResetInitialProd() { s.producer.ResetInitialProd() }
func (s *ConcurrentMeteredSink[K, V]) PrintRemainingStats() {
	s.lat.PrintRemainingStats()
	s.eventTimeSample.PrintRemainingStats()
}
func (s *ConcurrentMeteredSink[K, V]) NumCtrlMsg() uint64 {
	return atomic.LoadUint64(&s.ctrlCount)
}

type MeteredProducer[K, V any] struct {
	producer        *ShardedSharedLogStreamProducer[K, V]
	latencies       stats.StatsCollector[int64]
	eventTimeSample stats.StatsCollector[int64]
	produceTp       stats.ThroughputCounter
	warmup          stats.Warmup
	ctrlCount       uint64
	measure         bool
}

func NewMeteredProducer[K, V any](sink *ShardedSharedLogStreamProducer[K, V], warmup time.Duration) *MeteredProducer[K, V] {
	sink_name := fmt.Sprintf("%s_sink", sink.TopicName())
	return &MeteredProducer[K, V]{
		producer: sink,
		// eventTimeLatencies: make([]int, 0),
		latencies:       stats.NewStatsCollector[int64](sink_name, stats.DEFAULT_COLLECT_DURATION),
		produceTp:       stats.NewThroughputCounter(sink_name, stats.DEFAULT_COLLECT_DURATION),
		eventTimeSample: stats.NewStatsCollector[int64](sink_name+"_ets", stats.DEFAULT_COLLECT_DURATION),
		measure:         checkMeasureSink(),
		warmup:          stats.NewWarmupChecker(warmup),
	}
}

func (s *MeteredProducer[K, V]) InitFlushTimer() {}

func (s *MeteredProducer[K, V]) MarkFinalOutput() {
	s.producer.MarkFinalOutput()
}

func (s *MeteredProducer[K, V]) IsFinalOutput() bool {
	return s.producer.IsFinalOutput()
}

func (s *MeteredProducer[K, V]) StartWarmup() {
	if s.measure {
		s.warmup.StartWarmup()
	}
}

func (s *MeteredProducer[K, V]) Produce(ctx context.Context, msg commtypes.Message, parNum uint8, isControl bool) error {
	if s.IsFinalOutput() {
		procStart := time.Now()
		ts, err := extractEventTs(&msg)
		if err != nil {
			return err
		}
		if ts != 0 {
			els := procStart.UnixMilli() - ts
			// s.eventTimeLatencies = append(s.eventTimeLatencies, els)
			s.eventTimeSample.AddSample(els)
		}
	}
	assignInjTime(&msg)
	s.produceTp.Tick(1)
	if isControl {
		s.ctrlCount += 1
	}

	// if s.measure {
	// 	s.warmup.Check()
	// 	if s.warmup.AfterWarmup() {
	// 		if s.isFinalOutput {
	// 			procStart := time.Now()
	// 			ts, err := extractEventTs(&msg)
	// 			if err != nil {
	// 				return err
	// 			}
	// 			if ts != 0 {
	// 				els := int(procStart.UnixMilli() - ts)
	// 				s.eventTimeLatencies = append(s.eventTimeLatencies, els)
	// 			}
	// 		}
	// 	}
	// }
	procStart := stats.TimerBegin()
	err := s.producer.Produce(ctx, msg, parNum, isControl)
	elapsed := stats.Elapsed(procStart).Microseconds()
	s.latencies.AddSample(elapsed)
	return err
}

func (s *MeteredProducer[K, V]) TopicName() string               { return s.producer.TopicName() }
func (s *MeteredProducer[K, V]) Name() string                    { return s.producer.Name() }
func (s *MeteredProducer[K, V]) SetName(name string)             { s.producer.SetName(name) }
func (s *MeteredProducer[K, V]) KeyEncoder() commtypes.Encoder   { return s.producer.KeyEncoder() }
func (s *MeteredProducer[K, V]) Flush(ctx context.Context) error { return s.producer.Flush(ctx) }
func (s *MeteredProducer[K, V]) ConfigExactlyOnce(rem exactly_once_intr.ReadOnlyExactlyOnceManager,
	guarantee exactly_once_intr.GuaranteeMth,
) {
	s.producer.ConfigExactlyOnce(rem, guarantee)
}
func (s *MeteredProducer[K, V]) Stream() sharedlog_stream.Stream {
	return s.producer.Stream()
}
func (s *MeteredProducer[K, V]) GetInitialProdSeqNum(substreamNum uint8) uint64 {
	return s.producer.GetInitialProdSeqNum(substreamNum)
}
func (s *MeteredProducer[K, V]) GetCurrentProdSeqNum(substreamNum uint8) uint64 {
	return s.producer.GetCurrentProdSeqNum(substreamNum)
}
func (s *MeteredProducer[K, V]) ResetInitialProd() {
	s.producer.ResetInitialProd()
}
func (s *MeteredProducer[K, V]) PrintRemainingStats() {
	s.latencies.PrintRemainingStats()
	s.eventTimeSample.PrintRemainingStats()
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

// func (s *MeteredProducer[K, V]) GetEventTimeLatency() []int {
// 	return s.eventTimeLatencies
// }

func (s *MeteredProducer[K, V]) GetCount() uint64 {
	return s.produceTp.GetCount()
}

func (s *MeteredProducer[K, V]) NumCtrlMsg() uint64 {
	return s.ctrlCount
}
