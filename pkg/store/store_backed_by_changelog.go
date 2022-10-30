package store

import (
	"context"
	"sharedlog-stream/pkg/exactly_once_intr"
	"sharedlog-stream/pkg/sharedlog_stream"
)

type StoreBackedByChangelog interface {
	Flush(ctx context.Context) (uint32, error)
	ConfigureExactlyOnce(rem exactly_once_intr.ReadOnlyExactlyOnceManager,
		guarantee exactly_once_intr.GuaranteeMth) error
	ChangelogTopicName() string
	Stream() sharedlog_stream.Stream
}

type RestoreKVStore interface {
	ConsumeOneLogEntry(ctx context.Context, parNum uint8, consumedOffset uint64) (int, error)
}

type RestoreWindowStateStore interface {
	ConsumeOneLogEntry(ctx context.Context, parNum uint8) (int, error)
}
