package processor

type Processor interface {
	// WithPipe sets the pipe on the Processor.
	WithPipe(Pipe)
	// WithProcessorContext sets the context on the processor
	WithProcessorContext(ProcessorContext)
	// Process processes the stream Message.
	Process(Message) error
}
