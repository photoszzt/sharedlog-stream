package store

import (
	"context"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
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
		// debug.Fprintf(os.Stderr, "found %v\n", segmentId)
		kv := kvs.segments.Get(Int64(segmentId)).(*KeySegment)
		return kv.Value, nil
	} else {
		kv, err := NewMongoDBKeyValueSegment(ctx, kvs.mkvs, kvs.BaseSegments.SegmentName(segmentId),
			kvs.BaseSegments.name)
		if err != nil {
			return nil, err
		}
		// debug.Fprintf(os.Stderr, "inserting k: %v, v: %v\n", segmentId, kv.segmentName)
		_ = kvs.segments.ReplaceOrInsert(&KeySegment{Key: Int64(segmentId), Value: kv})
		return kv, nil
	}
}

func (kvs *MongoDBKeyValueSegments) CleanupExpiredMeta(ctx context.Context, expired []*KeySegment) error {
	return nil
}

func (kvs *MongoDBKeyValueSegments) GetSegmentNamesFromRemote(ctx context.Context) ([]string, error) {
	col := kvs.mkvs.client.Database(kvs.mkvs.config.DBName).Collection(kvs.BaseSegments.name)
	var result bson.M
	err := col.FindOne(ctx, bson.D{{Key: "_id", Value: 1}}).Decode(&result)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, nil
		}
		return nil, err
	}
	res := result[ALL_SEGS].([]string)
	return res, nil
}
