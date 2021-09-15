package processor

type StreamAggregateProcessor struct {
	pipe  Pipe
	store KeyValueStore
	pctx  ProcessorContext
}

var _ = Processor(&StreamAggregateProcessor{})

func (p *StreamAggregateProcessor) WithPipe(pipe Pipe) {
	p.pipe = pipe
}

func (p *StreamAggregateProcessor) WithProcessorContext(pctx ProcessorContext) {
	p.pctx = pctx
}

func (p *StreamAggregateProcessor) Process(Message) error {
	panic("not implemented")
}
