package processor

type StreamFilterProcessor struct {
	pipe Pipe
	pctx ProcessorContext
	pred Predicate
}

func NewStreamFilterProcessor(pred Predicate) Processor {
	return &StreamFilterProcessor{
		pred: pred,
	}
}

func (p *StreamFilterProcessor) WithProcessorContext(pctx ProcessorContext) {
	p.pctx = pctx
}

func (p *StreamFilterProcessor) WithPipe(pipe Pipe) {
	p.pipe = pipe
}

func (p *StreamFilterProcessor) Process(msg Message) error {
	ok, err := p.pred.Assert(&msg)
	if err != nil {
		return err
	}
	if ok {
		return p.pipe.Forward(msg)
	}
	return nil
}

type StreamFilterNotProcessor struct {
	pipe Pipe
	pctx ProcessorContext
	pred Predicate
}

func NewStreamFilterNotProcessor(pred Predicate) Processor {
	return &StreamFilterNotProcessor{
		pred: pred,
	}
}

func (p *StreamFilterNotProcessor) WithProcessorContext(pctx ProcessorContext) {
	p.pctx = pctx
}

func (p *StreamFilterNotProcessor) WithPipe(pipe Pipe) {
	p.pipe = pipe
}

func (p *StreamFilterNotProcessor) Process(msg Message) error {
	ok, err := p.pred.Assert(&msg)
	if err != nil {
		return err
	}
	if !ok {
		return p.pipe.Forward(msg)
	}
	return nil
}
