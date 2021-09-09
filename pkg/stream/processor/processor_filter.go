package processor

type FilterProcessor struct {
	pipe Pipe
	pred Predicate
}

func NewFilterProcessor(pred Predicate) Processor {
	return &FilterProcessor{
		pred: pred,
	}
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
