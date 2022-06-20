package processor

import (
	"context"
	"fmt"
	"sharedlog-stream/pkg/commtypes"
)

// PrintProcessor is a processor that prints the stream to stdout.
type PrintProcessor struct {
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

func (p *PrintProcessor) ProcessAndReturn(ctx context.Context, msg commtypes.Message) ([]commtypes.Message, error) {
	fmt.Printf("%v:%v\n", msg.Key, msg.Value)
	return []commtypes.Message{msg}, nil
}
