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
}

func NewMeteredProcessor(proc Processor) *MeteredProcessor {
	return &MeteredProcessor{
		proc:      proc,
		latencies: make([]int, 0, 128),
	}
}

func (p *MeteredProcessor) ProcessAndReturn(ctx context.Context, msg commtypes.Message) ([]commtypes.Message, error) {
	measure_proc := os.Getenv("MEASURE_PROC")
	if measure_proc == "true" || measure_proc == "1" {
		procStart := time.Now()
		newMsg, err := p.proc.ProcessAndReturn(ctx, msg)
		elapsed := time.Since(procStart)
		p.latencies = append(p.latencies, int(elapsed.Microseconds()))
		return newMsg, err
	} else {
		return p.proc.ProcessAndReturn(ctx, msg)
	}
}

func (p *MeteredProcessor) GetLatency() []int {
	return p.latencies
}
