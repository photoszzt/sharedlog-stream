package processor

type StreamJoinWindowProcessor struct {
	pipe       Pipe
	winStore   WindowStore
	pctx       ProcessorContext
	windowName string
}

var _ = Processor(&StreamJoinWindowProcessor{})

func NewStreamJoinWindowProcessor(windowName string) *StreamJoinWindowProcessor {
	return &StreamJoinWindowProcessor{
		windowName: windowName,
	}
}

// WithPipe sets the pipe on the Processor.
func (p *StreamJoinWindowProcessor) WithPipe(pipe Pipe) {
	p.pipe = pipe
}

// WithProcessorContext sets the context on the processor
func (p *StreamJoinWindowProcessor) WithProcessorContext(pctx ProcessorContext) {
	p.pctx = pctx
	p.winStore = p.pctx.GetWindowStore(p.windowName)
}

// Process processes the stream Message.
func (p *StreamJoinWindowProcessor) Process(msg Message) error {
	if msg.Key != nil {
		p.winStore.Put(msg.Key, msg.Value, msg.Timestamp)
		p.pipe.Forward(msg)
	}
	return nil
}
