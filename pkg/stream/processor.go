package stream

type Processor interface {
	// WithPipe sets the pipe on the Processor.
	WithPipe(Pipe)
	// Process processes the stream Message.
	Process(Message) error
}

var _ = (Predicate)(PredicateFunc(nil))

type PredicateFunc func(Message) (bool, error)

type Predicate interface {
	Assert(Message) (bool, error)
}

func (fn PredicateFunc) Assert(msg Message) (bool, error) {
	return fn(msg)
}

type MapperFunc func(Message) (Message, error)
