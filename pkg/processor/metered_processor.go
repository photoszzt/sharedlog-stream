package processor

import (
	"context"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/stats"

	"4d63.com/optional"
)

type MeteredProcessor struct {
	proc      Processor
	latencies *stats.ConcurrentStatsCollector[int64]
}

func NewMeteredProcessor(proc Processor) *MeteredProcessor {
	return &MeteredProcessor{
		proc:      proc,
		latencies: stats.NewConcurrentStatsCollector[int64](proc.Name(), stats.DEFAULT_COLLECT_DURATION),
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

func (p *MeteredProcessor) Process(ctx context.Context, msg commtypes.Message) error {
	return p.proc.Process(ctx, msg)
}

func (p *MeteredProcessor) InnerProcessor() Processor {
	return p.proc
}

type MeteredProcessorG[KIn, VIn, KOut, VOut any] struct {
	proc      ProcessorG[KIn, VIn, KOut, VOut]
	latencies *stats.ConcurrentStatsCollector[int64]
}

func (p *MeteredProcessorG[KIn, VIn, KOut, VOut]) Name() string { return p.proc.Name() }
func (p *MeteredProcessorG[KIn, VIn, KOut, VOut]) ProcessAndReturn(ctx context.Context,
	msg commtypes.MessageG[optional.Optional[KIn], optional.Optional[VIn]],
) ([]commtypes.MessageG[optional.Optional[KOut], optional.Optional[VOut]], error) {
	procStart := stats.TimerBegin()
	msgs, err := p.proc.ProcessAndReturn(ctx, msg)
	elapsed := stats.Elapsed(procStart).Microseconds()
	p.latencies.AddSample(elapsed)
	return msgs, err
}

func (p *MeteredProcessorG[KIn, VIn, KOut, VOut]) Process(ctx context.Context,
	msg commtypes.MessageG[optional.Optional[KIn], optional.Optional[VIn]],
) error {
	return p.proc.Process(ctx, msg)
}
