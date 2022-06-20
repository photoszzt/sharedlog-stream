package processor

import (
	"context"
	"sharedlog-stream/pkg/commtypes"
)

type Processor interface {
	// WithPipe sets the pipe on the Processor.
	WithPipe(Pipe)
	Name() string
	// Process processes the stream commtypes.Message.
	Process(context.Context, commtypes.Message) error
	ProcessAndReturn(context.Context, commtypes.Message) ([]commtypes.Message, error)
}
