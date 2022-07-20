package store

import "context"

type Segments[KeyT, ValT any] interface {
	SegmentId(ts int64) int64
	SegmentName(segmentId int64) string
	GetSegmentForTimestamp(ts int64) Segment[KeyT, ValT]
	GetOrCreateSegmentIfLive(ctx context.Context, segmentId int64,
		streamTime int64,
		getOrCreateSegment func(ctx context.Context, segmentId int64) (Segment[KeyT, ValT], error),
		cleanupExpired func(ctx context.Context, expired []*KeySegment) error,
	) (Segment[KeyT, ValT], error)
	GetOrCreateSegment(ctx context.Context, segmentId int64) (Segment[KeyT, ValT], error)
	Segments(timeFrom int64, timeTo int64) []Segment[KeyT, ValT]
	CleanupExpiredMeta(ctx context.Context, expired []*KeySegment) error
	GetSegmentNamesFromRemote(ctx context.Context) ([]string, error)
	Destroy(ctx context.Context) error
	StartTransaction(ctx context.Context) error
	CommitTransaction(ctx context.Context, taskRepr string, transactionalID uint64) error
	AbortTransaction(ctx context.Context) error
	GetTransactionID(ctx context.Context, taskRepr string) (uint64, bool, error)
}
