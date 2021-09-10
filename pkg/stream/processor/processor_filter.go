package processor

type FilterProcessor struct {
	pipe Pipe
	pctx ProcessorContext
	pred Predicate
}

func NewFilterProcessor(pred Predicate) Processor {
	return &FilterProcessor{
		pred: pred,
	}
}

func (p *FilterProcessor) WithProcessorContext(pctx ProcessorContext) {
	p.pctx = pctx
}

func (p *FilterProcessor) WithPipe(pipe Pipe) {
	p.pipe = pipe
}

func (p *FilterProcessor) Process(msg Message) error {
	ok, err := p.pred.Assert(msg)
	if err != nil {
		return err
	}
	if ok {
		return p.pipe.Forward(msg)
	}
	return nil
}
