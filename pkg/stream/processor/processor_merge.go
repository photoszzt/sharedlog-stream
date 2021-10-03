package processor

type MergeProcessor struct {
	pipe Pipe
	pctx ProcessorContext
}

func NewMergeProcessor() Processor {
	return &MergeProcessor{}
}

func (p *MergeProcessor) WithPipe(pipe Pipe) {
	p.pipe = pipe
}

func (p *MergeProcessor) WithProcessorContext(pctx ProcessorContext) {
	p.pctx = pctx
}

func (p *MergeProcessor) Process(msg Message) error {
	return p.pipe.Forward(msg)
}

func (p *MergeProcessor) ProcessAndReturn(msg Message) (*Message, error) {
	panic("not implemented")
}
