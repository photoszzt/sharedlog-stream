package processor

type StreamJoinWindowProcessor struct {
	pipe     Pipe
	winStore WindowStore
	pctx     ProcessorContext
}

var _ = Processor(&StreamJoinWindowProcessor{})

// WithPipe sets the pipe on the Processor.
func (p *StreamJoinWindowProcessor) WithPipe(pipe Pipe) {
	p.pipe = pipe
}

// WithProcessorContext sets the context on the processor
func (p *StreamJoinWindowProcessor) WithProcessorContext(pctx ProcessorContext) {
	p.pctx = pctx
}

// Process processes the stream Message.
func (p *StreamJoinWindowProcessor) Process(msg Message) error {
	if msg.Key != nil {
		p.winStore.Put(msg.Key, msg.Value, msg.Timestamp)
		p.pipe.Forward(msg)
	}
	return nil
}
