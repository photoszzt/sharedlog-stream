package processor

import (
	"context"
	"sharedlog-stream/pkg/commtypes"
)

type Processor interface {
	Name() string
	// Process processes the stream commtypes.Message.
	ProcessAndReturn(context.Context, commtypes.Message) ([]commtypes.Message, error)
}
