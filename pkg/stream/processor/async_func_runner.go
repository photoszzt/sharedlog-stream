package processor

import (
	"context"
	"sharedlog-stream/pkg/stream/processor/commtypes"

	"golang.org/x/sync/errgroup"
)

type AsyncFuncRunner struct {
	group  *errgroup.Group
	f      func(context.Context, commtypes.Message) ([]commtypes.Message, error)
	input  chan interface{}
	output chan []commtypes.Message
}

func NewAsyncFuncRunner(ctx context.Context,
	f func(context.Context, commtypes.Message) ([]commtypes.Message, error),
) *AsyncFuncRunner {
	pr := &AsyncFuncRunner{
		f:     f,
		input: make(chan interface{}),
	}
	g, ectx := errgroup.WithContext(ctx)
	pr.group = g
	pr.group.Go(func() error {
		return pr.run(ectx)
	})
	return pr
}

func (p *AsyncFuncRunner) run(ctx context.Context) error {
	for m := range p.input {
		msg := m.(commtypes.Message)
		msgs, err := p.f(ctx, msg)
		if err != nil {
			return err
		}
		if msgs != nil {
			p.output <- msgs
		}
	}
	return nil
}

func (p *AsyncFuncRunner) Accept(msg commtypes.Message) {
	p.input <- msg
}

func (p *AsyncFuncRunner) OutChan() chan []commtypes.Message {
	return p.output
}

func (p *AsyncFuncRunner) Wait() error {
	close(p.input)
	if err := p.group.Wait(); err != nil {
		return err
	}
	return nil
}
