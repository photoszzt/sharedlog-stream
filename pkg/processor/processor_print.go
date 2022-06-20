package processor

import (
	"context"
	"fmt"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/store"
)

// PrintProcessor is a processor that prints the stream to stdout.
type PrintProcessor struct {
	pipe Pipe
	pctx store.StoreContext
	name string
}

// NewPrintProcessor creates a new PrintProcessor instance.
func NewPrintProcessor(name string) Processor {
	return &PrintProcessor{
		name: name,
	}
}

func (p *PrintProcessor) Name() string {
	return p.name
}

// WithPipe sets the pipe on the Processor.
func (p *PrintProcessor) WithPipe(pipe Pipe) {
	p.pipe = pipe
}

func (p *PrintProcessor) WithProcessorContext(pctx store.StoreContext) {
	p.pctx = pctx
}

// Process processes the stream commtypes.Message.
func (p *PrintProcessor) Process(ctx context.Context, msg commtypes.Message) error {
	fmt.Printf("%v:%v\n", msg.Key, msg.Value)

	return p.pipe.Forward(ctx, msg)
}

func (p *PrintProcessor) ProcessAndReturn(ctx context.Context, msg commtypes.Message) ([]commtypes.Message, error) {
	fmt.Printf("%v:%v\n", msg.Key, msg.Value)
	return []commtypes.Message{msg}, nil
}
