package processor

import (
	"context"
	"fmt"
	"os"
	"sharedlog-stream/pkg/commtypes"
)

type PrintProcessor struct {
}

var _ Processor[any, any, any, any] = PrintProcessor{}

func (p PrintProcessor) Name() string {
	return "PrintProcessor"
}

func (p PrintProcessor) ProcessAndReturn(ctx context.Context, msg commtypes.Message[any, any]) ([]commtypes.Message[any, any], error) {
	fmt.Fprintf(os.Stderr, "msg: %v\n", msg)
	return []commtypes.Message[any, any]{msg}, nil
}
