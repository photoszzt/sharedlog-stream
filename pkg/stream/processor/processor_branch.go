package processor

import (
	"context"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/store"
)

// Branch processor
type BranchProcessor struct {
	pipe  Pipe
	pctx  store.StoreContext
	preds []Predicate
}

func NewBranchProcessor(preds []Predicate) Processor {
	return &BranchProcessor{
		preds: preds,
	}
}

// WithPipe sets the pipe on the Processor.
func (p *BranchProcessor) WithPipe(pipe Pipe) {
	p.pipe = pipe
}

func (p *BranchProcessor) WithProcessorContext(ctx store.StoreContext) {
	p.pctx = ctx
}

func (p *BranchProcessor) Process(ctx context.Context, msg commtypes.Message) error {
	for i, pred := range p.preds {
		ok, err := pred.Assert(&msg)
		if err != nil {
			return err
		}
		if !ok {
			continue
		}

		if err := p.pipe.ForwardToChild(ctx, msg, i); err != nil {
			return err
		}
	}
	return nil
}

func (p *BranchProcessor) ProcessAndReturn(ctx context.Context, msg commtypes.Message) ([]commtypes.Message, error) {
	panic("not implemented")
}
