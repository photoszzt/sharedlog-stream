package store

import "context"

type Stream interface {
	Push(ctx context.Context, payload []byte, parNum uint8, additionalTag []uint64) (uint64, error)
	ReadNext(ctx context.Context, parNum uint8) ([]byte /* payload */, uint64 /* seq num */, error)
	TopicName() string
	InitStream(ctx context.Context) error
}
