package processor

import (
	"context"
	"fmt"
	"os"
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
		input: make(chan interface{}, 1),
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
		fmt.Fprintf(os.Stderr, "got msg: %v\n", m)
		msg := m.(commtypes.Message)
		msgs, err := p.f(ctx, msg)
		if err != nil {
			return err
		}
		if len(msgs) != 0 {
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

func (p *AsyncFuncRunner) Close() {
	close(p.input)
}

func (p *AsyncFuncRunner) Wait() error {
	if err := p.group.Wait(); err != nil {
		return err
	}
	return nil
}
