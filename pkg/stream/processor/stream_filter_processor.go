package processor

type StreamFilterProcessor struct {
	pipe Pipe
	pctx ProcessorContext
	pred Predicate
}

var _ = Processor(&StreamFilterProcessor{})

func NewStreamFilterProcessor(pred Predicate) *StreamFilterProcessor {
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

func (p *StreamFilterProcessor) ProcessAndReturn(msg Message) ([]Message, error) {
	ok, err := p.pred.Assert(&msg)
	if err != nil {
		return nil, err
	}
	if ok {
		return []Message{msg}, nil
	}
	return nil, nil
}

type StreamFilterNotProcessor struct {
	pipe Pipe
	pctx ProcessorContext
	pred Predicate
}

var _ = Processor(&StreamFilterNotProcessor{})

func NewStreamFilterNotProcessor(pred Predicate) *StreamFilterNotProcessor {
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

func (p *StreamFilterNotProcessor) ProcessAndReturn(msg Message) ([]Message, error) {
	ok, err := p.pred.Assert(&msg)
	if err != nil {
		return nil, err
	}
	if !ok {
		return []Message{msg}, nil
	}
	return nil, nil
}
