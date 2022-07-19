package processor

import (
	"context"
	"sharedlog-stream/pkg/commtypes"
)

type Processor[KIn, VIn, KOut, VOut any] interface {
	Name() string
	// Process processes the stream commtypes.Message.
	ProcessAndReturn(context.Context, commtypes.Message[KIn, VIn]) ([]commtypes.Message[KOut, VOut], error)
}
