package processor

import (
	"context"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/stats"
)

type MeteredProcessor struct {
	proc      Processor
	latencies *stats.ConcurrentInt64Collector
}

func NewMeteredProcessor(proc Processor) *MeteredProcessor {
	return &MeteredProcessor{
		proc:      proc,
		latencies: stats.NewConcurrentInt64Collector(proc.Name(), stats.DEFAULT_COLLECT_DURATION),
	}
}

func (p *MeteredProcessor) Name() string {
	return p.proc.Name()
}

func (p *MeteredProcessor) ProcessAndReturn(ctx context.Context, msg commtypes.Message) ([]commtypes.Message, error) {
	procStart := stats.TimerBegin()
	msgs, err := p.proc.ProcessAndReturn(ctx, msg)
	elapsed := stats.Elapsed(procStart).Microseconds()
	p.latencies.AddSample(elapsed)
	return msgs, err
}

func (p *MeteredProcessor) InnerProcessor() Processor {
	return p.proc
}
