package processor

type Processor interface {
	// WithPipe sets the pipe on the Processor.
	WithPipe(Pipe)
	// Process processes the stream Message.
	Process(Message) error
}

type MapperFunc func(Message) (Message, error)
