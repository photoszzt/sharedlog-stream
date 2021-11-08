package processor

import (
	"context"
	"sharedlog-stream/pkg/stream/processor/commtypes"
)

type Source interface {
	// Consume gets the next commtypes.Message from the source
	Consume(ctx context.Context, parNum uint8) (commtypes.Message, uint64, error)
}
