package processor

import (
	"context"
	"sharedlog-stream/pkg/store"
	"sharedlog-stream/pkg/stream/processor/commtypes"
)

type Processor interface {
	// WithPipe sets the pipe on the Processor.
	WithPipe(Pipe)
	// WithProcessorContext sets the context on the processor
	WithProcessorContext(store.StoreContext)
	// Process processes the stream commtypes.Message.
	Process(context.Context, commtypes.Message) error
	ProcessAndReturn(context.Context, commtypes.Message) ([]commtypes.Message, error)
}
