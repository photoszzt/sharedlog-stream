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

type ConcurrentMeteredSink struct {
	// mu syncutils.Mutex
	// eventTimeLatencies []int

	producer *ShardedSharedLogStreamProducer

	produceTp       *stats.ConcurrentThroughputCounter
	lat             *stats.ConcurrentStatsCollector[int64]
	eventTimeSample *stats.ConcurrentStatsCollector[int64]
	warmup          stats.WarmupGoroutineSafe
	ctrlCount       uint32

	measure bool
}

var _ = MeteredProducerIntr(&ConcurrentMeteredSink{})

func NewConcurrentMeteredSyncProducer(sink *ShardedSharedLogStreamProducer, warmup time.Duration) *ConcurrentMeteredSink {
	sink_name := fmt.Sprintf("%s_sink", sink.TopicName())
	return &ConcurrentMeteredSink{
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

func (s *ConcurrentMeteredSink) MarkFinalOutput() {
	s.producer.MarkFinalOutput()
}

func (s *ConcurrentMeteredSink) IsFinalOutput() bool {
	return s.producer.IsFinalOutput()
}

func (s *ConcurrentMeteredSink) StartWarmup() {
	if s.measure {
		s.warmup.StartWarmup()
	}
}

// func (s *ConcurrentMeteredSink) GetEventTimeLatency() []int {
// 	return s.eventTimeLatencies
// }

func (s *ConcurrentMeteredSink) GetCount() uint64 {
	return s.produceTp.GetCount()
}

func (s *ConcurrentMeteredSink) InitFlushTimer() {}

func (s *ConcurrentMeteredSink) ProduceData(ctx context.Context, msgSer commtypes.MessageSerialized, parNum uint8) error {
	if s.IsFinalOutput() {
		procStart := time.Now()
		ts := msgSer.Timestamp
		if ts != 0 {
			els := procStart.UnixMilli() - ts
			// s.mu.Lock()
			// s.eventTimeLatencies = append(s.eventTimeLatencies, els)
			// s.mu.Unlock()
			s.eventTimeSample.AddSample(els)
		}
	}
	assignInjTime(&msgSer)
	s.produceTp.Tick(1)
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
	err := s.producer.ProduceData(ctx, msgSer, parNum)
	elapsed := stats.Elapsed(procStart).Microseconds()
	s.lat.AddSample(elapsed)
	return err
}

func (s *ConcurrentMeteredSink) ProduceCtrlMsg(ctx context.Context, msg commtypes.RawMsgAndSeq, parNums []uint8) (int, error) {
	return s.producer.ProduceCtrlMsg(ctx, msg, parNums)
}

func (s *ConcurrentMeteredSink) TopicName() string               { return s.producer.TopicName() }
func (s *ConcurrentMeteredSink) Name() string                    { return s.producer.Name() }
func (s *ConcurrentMeteredSink) SetName(name string)             { s.producer.SetName(name) }
func (s *ConcurrentMeteredSink) Flush(ctx context.Context) error { return s.producer.Flush(ctx) }
func (s *ConcurrentMeteredSink) ConfigExactlyOnce(rem exactly_once_intr.ReadOnlyExactlyOnceManager,
	guarantee exactly_once_intr.GuaranteeMth,
) {
	s.producer.ConfigExactlyOnce(rem, guarantee)
}
func (s *ConcurrentMeteredSink) Stream() sharedlog_stream.Stream {
	return s.producer.Stream()
}
func (s *ConcurrentMeteredSink) GetInitialProdSeqNum(substreamNum uint8) uint64 {
	return s.producer.GetInitialProdSeqNum(substreamNum)
}
func (s *ConcurrentMeteredSink) GetCurrentProdSeqNum(substreamNum uint8) uint64 {
	return s.producer.GetCurrentProdSeqNum(substreamNum)
}
func (s *ConcurrentMeteredSink) ResetInitialProd() { s.producer.ResetInitialProd() }
func (s *ConcurrentMeteredSink) PrintRemainingStats() {
	s.lat.PrintRemainingStats()
	s.eventTimeSample.PrintRemainingStats()
}
func (s *ConcurrentMeteredSink) NumCtrlMsg() uint32 {
	return atomic.LoadUint32(&s.ctrlCount)
}

type MeteredProducer struct {
	producer        *ShardedSharedLogStreamProducer
	latencies       stats.StatsCollector[int64]
	eventTimeSample stats.StatsCollector[int64]
	produceTp       stats.ThroughputCounter
	warmup          stats.Warmup
	ctrlCount       uint32
	measure         bool
}

func NewMeteredProducer(sink *ShardedSharedLogStreamProducer, warmup time.Duration) *MeteredProducer {
	sink_name := fmt.Sprintf("%s_sink", sink.TopicName())
	return &MeteredProducer{
		producer: sink,
		// eventTimeLatencies: make([]int, 0),
		latencies:       stats.NewStatsCollector[int64](sink_name, stats.DEFAULT_COLLECT_DURATION),
		produceTp:       stats.NewThroughputCounter(sink_name, stats.DEFAULT_COLLECT_DURATION),
		eventTimeSample: stats.NewStatsCollector[int64](sink_name+"_ets", stats.DEFAULT_COLLECT_DURATION),
		measure:         checkMeasureSink(),
		warmup:          stats.NewWarmupChecker(warmup),
	}
}

func (s *MeteredProducer) InitFlushTimer() {}

func (s *MeteredProducer) MarkFinalOutput() {
	s.producer.MarkFinalOutput()
}

func (s *MeteredProducer) IsFinalOutput() bool {
	return s.producer.IsFinalOutput()
}

func (s *MeteredProducer) StartWarmup() {
	if s.measure {
		s.warmup.StartWarmup()
	}
}

func (s *MeteredProducer) ProduceCtrlMsg(ctx context.Context, msg commtypes.RawMsgAndSeq, parNums []uint8) (int, error) {
	ctrlCnt, err := s.producer.ProduceCtrlMsg(ctx, msg, parNums)
	s.ctrlCount += uint32(ctrlCnt)
	return ctrlCnt, err
}

func (s *MeteredProducer) ProduceData(ctx context.Context, msg commtypes.MessageSerialized, parNum uint8) error {
	if s.IsFinalOutput() {
		procStart := time.Now()
		ts := msg.Timestamp
		if msg.Timestamp != 0 {
			els := procStart.UnixMilli() - ts
			// s.eventTimeLatencies = append(s.eventTimeLatencies, els)
			s.eventTimeSample.AddSample(els)
		}
	}
	assignInjTime(&msg)
	s.produceTp.Tick(1)

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
	err := s.producer.ProduceData(ctx, msg, parNum)
	elapsed := stats.Elapsed(procStart).Microseconds()
	s.latencies.AddSample(elapsed)
	return err
}

func (s *MeteredProducer) TopicName() string               { return s.producer.TopicName() }
func (s *MeteredProducer) Name() string                    { return s.producer.Name() }
func (s *MeteredProducer) SetName(name string)             { s.producer.SetName(name) }
func (s *MeteredProducer) Flush(ctx context.Context) error { return s.producer.Flush(ctx) }
func (s *MeteredProducer) ConfigExactlyOnce(rem exactly_once_intr.ReadOnlyExactlyOnceManager,
	guarantee exactly_once_intr.GuaranteeMth,
) {
	s.producer.ConfigExactlyOnce(rem, guarantee)
}
func (s *MeteredProducer) Stream() sharedlog_stream.Stream {
	return s.producer.Stream()
}
func (s *MeteredProducer) GetInitialProdSeqNum(substreamNum uint8) uint64 {
	return s.producer.GetInitialProdSeqNum(substreamNum)
}
func (s *MeteredProducer) GetCurrentProdSeqNum(substreamNum uint8) uint64 {
	return s.producer.GetCurrentProdSeqNum(substreamNum)
}
func (s *MeteredProducer) ResetInitialProd() {
	s.producer.ResetInitialProd()
}
func (s *MeteredProducer) PrintRemainingStats() {
	s.latencies.PrintRemainingStats()
	s.eventTimeSample.PrintRemainingStats()
}

// func (s *MeteredProducer) GetEventTimeLatency() []int {
// 	return s.eventTimeLatencies
// }

func (s *MeteredProducer) GetCount() uint64 {
	return s.produceTp.GetCount()
}

func (s *MeteredProducer) NumCtrlMsg() uint32 {
	return s.ctrlCount
}
