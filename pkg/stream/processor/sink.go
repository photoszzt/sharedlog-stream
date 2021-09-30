package processor

type Sink interface {
	Sink(msg Message, parNum uint32) error
}
