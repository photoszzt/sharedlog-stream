package processor

import (
	"context"
	"sharedlog-stream/pkg/stream/processor/commtypes"
)

type Sink interface {
	Sink(ctx context.Context, msg commtypes.Message, parNum uint8, isControl bool) error
}
