package store

import (
	"context"
	"sharedlog-stream/pkg/stream/processor/commtypes"
)

type Stream interface {
	Push(ctx context.Context, payload []byte, parNum uint8, isControl bool) (uint64, error)
	ReadNext(ctx context.Context, parNum uint8) (commtypes.AppIDGen, []commtypes.RawMsg /* payload */, error)
	TopicName() string
	InitStream(ctx context.Context) error
}
