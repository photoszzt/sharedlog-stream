package processor

import (
	"context"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/store"
)

type MergeProcessor struct {
	pipe Pipe
	pctx store.StoreContext
}

func NewMergeProcessor() Processor {
	return &MergeProcessor{}
}

func (p *MergeProcessor) WithPipe(pipe Pipe) {
	p.pipe = pipe
}

func (p *MergeProcessor) WithProcessorContext(pctx store.StoreContext) {
	p.pctx = pctx
}

func (p *MergeProcessor) Process(ctx context.Context, msg commtypes.Message) error {
	return p.pipe.Forward(ctx, msg)
}

func (p *MergeProcessor) ProcessAndReturn(ctx context.Context, msg commtypes.Message) ([]commtypes.Message, error) {
	panic("not implemented")
}
