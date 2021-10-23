package processor

import "sharedlog-stream/pkg/stream/processor/commtypes"

type Source interface {
	// Consume gets the next commtypes.Message from the source
	Consume() (commtypes.Message, error)
}
