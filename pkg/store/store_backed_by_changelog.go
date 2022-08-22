package store

import (
	"context"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/exactly_once_intr"
	"sharedlog-stream/pkg/sharedlog_stream"
)

type StoreBackedByChangelog interface {
	Flush(ctx context.Context) error
	ConsumeChangelog(ctx context.Context, parNum uint8) (*commtypes.MsgAndSeqs, error)
	ConfigureExactlyOnce(rem exactly_once_intr.ReadOnlyExactlyOnceManager,
		guarantee exactly_once_intr.GuaranteeMth) error
	ChangelogTopicName() string
	Stream() sharedlog_stream.Stream
}
