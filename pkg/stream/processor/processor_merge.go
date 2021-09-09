package processor

type MergeProcessor struct {
	pipe Pipe
}

func NewMergeProcessor() Processor {
	return &MergeProcessor{}
}

func (p *MergeProcessor) WithPipe(pipe Pipe) {
	p.pipe = pipe
}

func (p *MergeProcessor) Process(msg Message) error {
	return p.pipe.Forward(msg)
}
