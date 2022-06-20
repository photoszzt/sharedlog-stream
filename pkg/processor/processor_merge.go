package processor

import (
	"context"
	"sharedlog-stream/pkg/commtypes"
)

type MergeProcessor struct {
	name string
}

func NewMergeProcessor(name string) Processor {
	return &MergeProcessor{
		name: name,
	}
}

func (p *MergeProcessor) Name() string {
	return p.name
}

func (p *MergeProcessor) ProcessAndReturn(ctx context.Context, msg commtypes.Message) ([]commtypes.Message, error) {
	panic("not implemented")
}
