package processor

import (
	"context"
	"sharedlog-stream/pkg/commtypes"
)

// Branch processor
type BranchProcessor struct {
	preds []Predicate
	name  string
}

func NewBranchProcessor(name string, preds []Predicate) Processor {
	return &BranchProcessor{
		preds: preds,
		name:  name,
	}
}

func (p *BranchProcessor) Name() string {
	return p.name
}

func (p *BranchProcessor) ProcessAndReturn(ctx context.Context, msg commtypes.Message) ([]commtypes.Message, error) {
	panic("not implemented")
}
