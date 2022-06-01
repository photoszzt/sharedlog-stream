package processor

import (
	"context"
	"sharedlog-stream/pkg/commtypes"

	"golang.org/x/xerrors"
)

type Pipe interface {
	// Forward passes the message with all processor children in the topology.
	Forward(context.Context, commtypes.Message) error
	// ForwardToChild passes the message with the given processor child in the topology.
	ForwardToChild(context.Context, commtypes.Message, int) error
}

type processorPipe struct {
	children []Pump
}

func NewPipe(children []Pump) Pipe {
	return &processorPipe{
		children: children,
	}
}

func (p *processorPipe) Forward(ctx context.Context, msg commtypes.Message) error {
	for _, child := range p.children {
		if err := child.Accept(ctx, msg); err != nil {
			return err
		}
	}
	return nil
}

func (p *processorPipe) ForwardToChild(ctx context.Context, msg commtypes.Message, index int) error {
	if index > len(p.children)-1 {
		return xerrors.New("stream: child index out of bounds")
	}

	child := p.children[index]
	err := child.Accept(ctx, msg)
	return err
}

/*
type pipeToStream struct {
	streamSink []Sink
}

func NewStreamPipe(streamSink []Sink) Pipe {
	return &pipeToStream{
		streamSink: streamSink,
	}
}

func (p *pipeToStream) Forward(msg Message) error {
	for _, sink := range p.streamSink {
		if err := sink.Sink()
	}
}
*/
