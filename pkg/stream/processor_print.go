package stream

import (
	"fmt"
)

// PrintProcessor is a processor that prints the stream to stdout.
type PrintProcessor struct {
	pipe Pipe
}

// NewPrintProcessor creates a new PrintProcessor instance.
func NewPrintProcessor() Processor {
	return &PrintProcessor{}
}

// WithPipe sets the pipe on the Processor.
func (p *PrintProcessor) WithPipe(pipe Pipe) {
	p.pipe = pipe
}

// Process processes the stream Message.
func (p *PrintProcessor) Process(msg Message) error {
	fmt.Printf("%v:%v\n", msg.Key, msg.Value)

	return p.pipe.Forward(msg)
}
