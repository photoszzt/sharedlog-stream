package stream

import (
	"golang.org/x/xerrors"
)

type Pipe interface {
	// Forward passes the message with all processor children in the topology.
	Forward(Message) error
	// ForwardToChild passes the message with the given processor child in the topology.
	ForwardToChild(Message, int) error
}

type processorPipe struct {
	proc     Processor
	children []Pump
}

func NewPipe(proc Processor, children []Pump) Pipe {
	return &processorPipe{
		proc:     proc,
		children: children,
	}
}

func (p *processorPipe) Forward(msg Message) error {
	for _, child := range p.children {
		if err := child.Accept(msg); err != nil {
			return err
		}
	}
	return nil
}

func (p *processorPipe) ForwardToChild(msg Message, index int) error {
	if index > len(p.children)-1 {
		return xerrors.New("stream: child index out of bounds")
	}

	child := p.children[index]
	err := child.Accept(msg)
	return err
}
