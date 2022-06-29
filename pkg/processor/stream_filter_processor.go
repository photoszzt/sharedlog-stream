package processor

import (
	"context"
	"sharedlog-stream/pkg/commtypes"
)

type StreamFilterProcessor struct {
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

func (p *StreamFilterProcessor) ProcessAndReturn(ctx context.Context, msg commtypes.Message) ([]commtypes.Message, error) {
	ok, err := p.pred.Assert(msg.Key, msg.Value)
	if err != nil {
		return nil, err
	}
	if ok {
		return []commtypes.Message{msg}, nil
	}
	return nil, nil
}

type StreamFilterNotProcessor struct {
	pred Predicate
	name string
}

var _ = Processor(&StreamFilterNotProcessor{})

func NewStreamFilterNotProcessor(name string, pred Predicate) *StreamFilterNotProcessor {
	return &StreamFilterNotProcessor{
		pred: pred,
	}
}

func (p *StreamFilterNotProcessor) Name() string {
	return p.name
}

func (p *StreamFilterNotProcessor) ProcessAndReturn(ctx context.Context, msg commtypes.Message) ([]commtypes.Message, error) {
	ok, err := p.pred.Assert(msg.Key, msg.Value)
	if err != nil {
		return nil, err
	}
	if !ok {
		return []commtypes.Message{msg}, nil
	}
	return nil, nil
}
