package store

import (
	"context"
	"fmt"
	"os"
)

type MongoDBKeyValueSegments struct {
	mkvs *MongoDBKeyValueStore
	BaseSegments
}

func NewMongoDBKeyValueSegments(name string, retentionPeriod int64,
	segmentInterval int64, mkvs *MongoDBKeyValueStore,
) *MongoDBKeyValueSegments {
	return &MongoDBKeyValueSegments{
		BaseSegments: *NewBaseSegments(name, retentionPeriod, segmentInterval),
		mkvs:         mkvs,
	}
}

func (kvs *MongoDBKeyValueSegments) GetOrCreateSegment(ctx context.Context, segmentId int64) (Segment, error) {
	if kvs.segments.Has(Int64(segmentId)) {
		fmt.Fprintf(os.Stderr, "found %v\n", segmentId)
		kv := kvs.segments.Get(Int64(segmentId)).(*KeySegment)
		return kv.Value, nil
	} else {
		kv := NewMongoDBKeyValueSegment(ctx, kvs.mkvs, kvs.BaseSegments.SegmentName(segmentId),
			kvs.BaseSegments.name)
		fmt.Fprintf(os.Stderr, "inserting k: %v, v: %v\n", segmentId, kv.segmentName)
		_ = kvs.segments.ReplaceOrInsert(&KeySegment{Key: Int64(segmentId), Value: kv})
		return kv, nil
	}
}
