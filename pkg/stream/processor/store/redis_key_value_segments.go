package store

import (
	"context"
	"fmt"
	"os"
)

type RedisKeyValueSegments struct {
	rkvs *RedisKeyValueStore
	BaseSegments
}

func NewRedisKeyValueSegments(name string, retentionPeriod int64,
	segmentInterval int64, rkvs *RedisKeyValueStore,
) *RedisKeyValueSegments {
	return &RedisKeyValueSegments{
		BaseSegments: *NewBaseSegments(name, retentionPeriod, segmentInterval),
		rkvs:         rkvs,
	}
}

func (kvs *RedisKeyValueSegments) GetOrCreateSegment(ctx context.Context, segmentId int64) (Segment, error) {
	if kvs.segments.Has(Int64(segmentId)) {
		fmt.Fprintf(os.Stderr, "found %v\n", segmentId)
		kv := kvs.segments.Get(Int64(segmentId)).(*KeySegment)
		return kv.Value, nil
	} else {
		kv, err := NewRedisKeyValueSegment(ctx, kvs.rkvs, kvs.BaseSegments.SegmentName(segmentId),
			kvs.BaseSegments.name)
		if err != nil {
			return nil, err
		}
		fmt.Fprintf(os.Stderr, "inserting k: %v, v: %v\n", segmentId, kv.segmentName)
		_ = kvs.segments.ReplaceOrInsert(&KeySegment{Key: Int64(segmentId), Value: kv})
		return kv, nil
	}
}

func (kvs *RedisKeyValueSegments) getSegments(ctx context.Context) ([]string, error) {
	return kvs.rkvs.rdb.ZRange(ctx, kvs.BaseSegments.name, 0, -1).Result()
}

func (kvs *RedisKeyValueSegments) CleanupExpiredMeta(ctx context.Context, expired []*KeySegment) error {
	segNames := make([]string, 0, len(expired))
	for _, s := range expired {
		segNames = append(segNames, s.Value.Name())
	}
	err := kvs.rkvs.rdb.ZRem(ctx, kvs.BaseSegments.name, segNames).Err()
	return err
}
