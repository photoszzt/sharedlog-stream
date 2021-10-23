package processor

import (
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"sharedlog-stream/pkg/stream/processor/store"
)

type MergeProcessor struct {
	pipe Pipe
	pctx store.ProcessorContext
}

func NewMergeProcessor() Processor {
	return &MergeProcessor{}
}

func (p *MergeProcessor) WithPipe(pipe Pipe) {
	p.pipe = pipe
}

func (p *MergeProcessor) WithProcessorContext(pctx store.ProcessorContext) {
	p.pctx = pctx
}

func (p *MergeProcessor) Process(msg commtypes.Message) error {
	return p.pipe.Forward(msg)
}

func (p *MergeProcessor) ProcessAndReturn(msg commtypes.Message) ([]commtypes.Message, error) {
	panic("not implemented")
}
