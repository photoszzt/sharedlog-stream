package processor

import (
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"sharedlog-stream/pkg/stream/processor/store"
)

type Processor interface {
	// WithPipe sets the pipe on the Processor.
	WithPipe(Pipe)
	// WithProcessorContext sets the context on the processor
	WithProcessorContext(store.ProcessorContext)
	// Process processes the stream commtypes.Message.
	Process(commtypes.Message) error
	ProcessAndReturn(commtypes.Message) ([]commtypes.Message, error)
}
