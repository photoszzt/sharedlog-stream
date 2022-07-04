package processor

import (
	"context"
	"fmt"
	"os"
	"sharedlog-stream/pkg/commtypes"
)

type PrintProcessor struct {
}

var _ Processor = PrintProcessor{}

func (p PrintProcessor) Name() string {
	return "PrintProcessor"
}

func (p PrintProcessor) ProcessAndReturn(ctx context.Context, msg commtypes.Message) ([]commtypes.Message, error) {
	fmt.Fprintf(os.Stderr, "msg: %v\n", msg)
	return []commtypes.Message{msg}, nil
}
