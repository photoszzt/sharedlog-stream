package processor

import (
	"context"
	"sharedlog-stream/pkg/stream/processor/commtypes"

	"golang.org/x/sync/errgroup"
)

type ProcessorRunner struct {
	group *errgroup.Group
	proc  Processor
	input chan interface{}
}

func NewProcessorRunner(ctx context.Context, p Processor) *ProcessorRunner {
	pr := &ProcessorRunner{
		proc:  p,
		input: make(chan interface{}),
	}
	g, ectx := errgroup.WithContext(ctx)
	pr.group = g
	pr.group.Go(func() error {
		return pr.run(ectx)
	})
	return pr
}

func (p *ProcessorRunner) run(ctx context.Context) error {
	for m := range p.input {
		msg := m.(commtypes.Message)
		err := p.proc.Process(ctx, msg)
		if err != nil {
			return err
		}
	}
	return nil
}

func (p *ProcessorRunner) Wait() error {
	close(p.input)
	if err := p.group.Wait(); err != nil {
		return err
	}
	return nil
}
