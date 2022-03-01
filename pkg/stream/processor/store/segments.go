package store

import "context"

type Segments interface {
	SegmentId(ts int64) int64
	SegmentName(segmentId int64) string
	GetSegmentForTimestamp(ts int64) Segment
	GetOrCreateSegmentIfLive(ctx context.Context, segmentId int64, streamTime int64,
		getOrCreateSegment func(ctx context.Context, segmentId int64) (Segment, error)) (Segment, error)
	GetOrCreateSegment(ctx context.Context, segmentId int64) (Segment, error)
	Segments(timeFrom int64, timeTo int64) []Segment
}
