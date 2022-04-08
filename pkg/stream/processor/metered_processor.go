package processor

import (
	"context"
	"os"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"time"
)

type MeteredProcessor struct {
	proc      Processor
	latencies []int
	measure   bool
}

func NewMeteredProcessor(proc Processor) *MeteredProcessor {
	measure_str := os.Getenv("MEASURE_PROC")
	measure := false
	if measure_str == "true" || measure_str == "1" {
		measure = true
	}
	return &MeteredProcessor{
		proc:      proc,
		latencies: make([]int, 0, 128),
		measure:   measure,
	}
}

func (p *MeteredProcessor) ProcessAndReturn(ctx context.Context, msg commtypes.Message) ([]commtypes.Message, error) {
	if p.measure {
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
