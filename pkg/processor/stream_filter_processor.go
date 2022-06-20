package processor

import (
	"context"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/store"
)

type StreamFilterProcessor struct {
	pipe Pipe
	pctx store.StoreContext
	pred Predicate
	name string
}

var _ = Processor(&StreamFilterProcessor{})

func NewStreamFilterProcessor(name string, pred Predicate) *StreamFilterProcessor {
	return &StreamFilterProcessor{
		pred: pred,
		name: name,
	}
}

func (p *StreamFilterProcessor) Name() string {
	return p.name
}

func (p *StreamFilterProcessor) WithProcessorContext(pctx store.StoreContext) {
	p.pctx = pctx
}

func (p *StreamFilterProcessor) WithPipe(pipe Pipe) {
	p.pipe = pipe
}

func (p *StreamFilterProcessor) Process(ctx context.Context, msg commtypes.Message) error {
	ok, err := p.pred.Assert(&msg)
	if err != nil {
		return err
	}
	if ok {
		return p.pipe.Forward(ctx, msg)
	}
	return nil
}

func (p *StreamFilterProcessor) ProcessAndReturn(ctx context.Context, msg commtypes.Message) ([]commtypes.Message, error) {
	ok, err := p.pred.Assert(&msg)
	if err != nil {
		return nil, err
	}
	if ok {
		return []commtypes.Message{msg}, nil
	}
	return nil, nil
}

type StreamFilterNotProcessor struct {
	pipe Pipe
	pctx store.StoreContext
	pred Predicate
	name string
}

var _ = Processor(&StreamFilterNotProcessor{})

func NewStreamFilterNotProcessor(name string, pred Predicate) *StreamFilterNotProcessor {
	return &StreamFilterNotProcessor{
		pred: pred,
	}
}

func (p *StreamFilterNotProcessor) WithProcessorContext(pctx store.StoreContext) {
	p.pctx = pctx
}

func (p *StreamFilterNotProcessor) WithPipe(pipe Pipe) {
	p.pipe = pipe
}

func (p *StreamFilterNotProcessor) Name() string {
	return p.name
}

func (p *StreamFilterNotProcessor) Process(ctx context.Context, msg commtypes.Message) error {
	ok, err := p.pred.Assert(&msg)
	if err != nil {
		return err
	}
	if !ok {
		return p.pipe.Forward(ctx, msg)
	}
	return nil
}

func (p *StreamFilterNotProcessor) ProcessAndReturn(ctx context.Context, msg commtypes.Message) ([]commtypes.Message, error) {
	ok, err := p.pred.Assert(&msg)
	if err != nil {
		return nil, err
	}
	if !ok {
		return []commtypes.Message{msg}, nil
	}
	return nil, nil
}
