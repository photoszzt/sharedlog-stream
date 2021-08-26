package stream

// Branch processor
type BranchProcessor struct {
	pipe  Pipe
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

func (p *BranchProcessor) Process(msg Message) error {
	for i, pred := range p.preds {
		ok, err := pred.Assert(msg)
		if err != nil {
			return err
		}
		if !ok {
			continue
		}

		if err := p.pipe.ForwardToChild(msg, i); err != nil {
			return err
		}
	}
	return nil
}
