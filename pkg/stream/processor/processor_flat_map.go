package processor

import (
	"context"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"sharedlog-stream/pkg/stream/processor/store"
)

type FlatMapper interface {
	FlatMap(commtypes.Message) ([]commtypes.Message, error)
}

var _ = (FlatMapper)(FlatMapperFunc(nil))

type FlatMapperFunc func(commtypes.Message) ([]commtypes.Message, error)

func (fn FlatMapperFunc) FlatMap(msg commtypes.Message) ([]commtypes.Message, error) {
	return fn(msg)
}

type FlatMapProcessor struct {
	pipe   Pipe
	mapper FlatMapper
	pctx   store.StoreContext
}

func NewFlatMapProcessor(mapper FlatMapper) *FlatMapProcessor {
	return &FlatMapProcessor{
		mapper: mapper,
	}
}

func (p *FlatMapProcessor) WithProcessorContext(pctx store.StoreContext) {
	p.pctx = pctx
}

func (p *FlatMapProcessor) WithPipe(pipe Pipe) {
	p.pipe = pipe
}

func (p *FlatMapProcessor) Process(ctx context.Context, msg commtypes.Message) error {
	msgs, err := p.mapper.FlatMap(msg)
	if err != nil {
		return err
	}
	for _, m := range msgs {
		m.Timestamp = msg.Timestamp
		if err := p.pipe.Forward(ctx, m); err != nil {
			return err
		}
	}
	return nil
}

func (p *FlatMapProcessor) ProcessAndReturn(ctx context.Context, msg commtypes.Message) ([]commtypes.Message, error) {
	msgs, err := p.mapper.FlatMap(msg)
	if err != nil {
		return nil, err
	}
	return msgs, nil
}
