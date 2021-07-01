package stream

// Input is a type that exposes one open input.
type Input interface {
	In() chan<- interface{}
}

// Output is a type that exposes one open output.
type Output interface {
	Out() <-chan interface{}
}

// A Source is a set of stream processing steps that has one open output
type Source interface {
	Output
}

// A Flow is a set of stream processing steps that has one open input and one open output
type Flow interface {
	Input
	Output
}

// A Sink is a set of stream processing steps that has one open input.
type Sink interface {
	Input
}
