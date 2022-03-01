package store

import "context"

type RedisKeyValueSegments struct {
	rkvs     *RedisKeyValueStore
	app_name string
	BaseSegments
}

func NewRedisKeyValueSegments(windowName string, app_name string, retentionPeriod int64, segmentInterval int64, rkvs *RedisKeyValueStore) *RedisKeyValueSegments {
	return &RedisKeyValueSegments{
		BaseSegments: *NewBaseSegments(windowName, retentionPeriod, segmentInterval),
		rkvs:         rkvs,
		app_name:     app_name,
	}
}

func (kvs *RedisKeyValueSegments) GetOrCreateSegment(ctx context.Context, segmentId int64) (Segment, error) {
	if kvs.segments.Has(Int64(segmentId)) {
		kv := kvs.segments.Get(Int64(segmentId)).(*KeySegment)
		return kv.Value, nil
	} else {
		kv, err := NewRedisKeyValueSegment(ctx, kvs.rkvs, kvs.BaseSegments.SegmentName(segmentId),
			kvs.BaseSegments.name, kvs.app_name)
		if err != nil {
			return nil, err
		}
		_ = kvs.segments.ReplaceOrInsert(&KeySegment{Key: Int64(segmentId), Value: kv})
		return kv, nil
	}
}
