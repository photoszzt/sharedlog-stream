package processor

import (
	"context"
	"os"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/debug"
	"time"
)

type MeteredProcessor struct {
	proc      Processor
	latencies []int
	warmup    time.Duration
	initial   time.Time
	measure   bool
}

func NewMeteredProcessor(proc Processor, warmup time.Duration) *MeteredProcessor {
	measure_str := os.Getenv("MEASURE_PROC")
	measure := false
	if measure_str == "true" || measure_str == "1" {
		measure = true
	}
	return &MeteredProcessor{
		proc:      proc,
		latencies: make([]int, 0, 128),
		measure:   measure,
		warmup:    warmup,
	}
}

func (p *MeteredProcessor) Name() string {
	return p.proc.Name()
}

func (p *MeteredProcessor) StartWarmup() {
	if p.measure {
		p.initial = time.Now()
	}
}

func (p *MeteredProcessor) ProcessAndReturn(ctx context.Context, msg commtypes.Message) ([]commtypes.Message, error) {
	debug.Assert(!p.measure || (p.warmup == 0 || (p.warmup > 0 && !p.initial.IsZero())), "warmup should initialize initial")
	if p.measure && (p.warmup == 0 || (p.warmup > 0 && time.Since(p.initial) >= p.warmup)) {
		procStart := time.Now()
		newMsg, err := p.proc.ProcessAndReturn(ctx, msg)
		elapsed := time.Since(procStart)
		p.latencies = append(p.latencies, int(elapsed.Microseconds()))
		return newMsg, err
	}
	return p.proc.ProcessAndReturn(ctx, msg)
}

func (p *MeteredProcessor) GetLatency() []int {
	return p.latencies
}

func (p *MeteredProcessor) InnerProcessor() Processor {
	return p.proc
}
