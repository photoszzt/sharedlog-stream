package stream

type Pipe interface {
	// Forward passes the message with all processor children in the topology.
	Forward(Message) error
	// ForwardToChild passes the message with the given processor child in the topology.
	ForwardToChild(Message, int) error
}
