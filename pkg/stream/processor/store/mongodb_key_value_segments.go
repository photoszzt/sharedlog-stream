package store

import (
	"context"
	"os"
	"sharedlog-stream/pkg/debug"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
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
		debug.Fprintf(os.Stderr, "inserting k: %v, v: %v\n", segmentId, kv.segmentName)
		_ = kvs.segments.ReplaceOrInsert(&KeySegment{Key: Int64(segmentId), Value: kv})
		return kv, nil
	}
}

func (kvs *MongoDBKeyValueSegments) CleanupExpiredMeta(ctx context.Context, expired []*KeySegment) error {
	segNames := make([]string, 0, len(expired))
	for _, s := range expired {
		segNames = append(segNames, s.Value.Name())
	}
	col := kvs.mkvs.client.Database(kvs.mkvs.config.DBName).Collection(kvs.BaseSegments.name)
	_, err := col.UpdateOne(ctx, bson.M{"_id": 1}, bson.M{"$pull": bson.M{ALL_SEGS: segNames}})
	if err != nil {
		return err
	}
	return nil
}

func (kvs *MongoDBKeyValueSegments) GetSegmentNamesFromRemote(ctx context.Context) ([]string, error) {
	col := kvs.mkvs.client.Database(kvs.mkvs.config.DBName).Collection(kvs.BaseSegments.name)
	debug.Fprintf(os.Stderr, "GetSegmentNamesFromRemote: using col %s from db %s\n", kvs.BaseSegments.name, kvs.mkvs.config.DBName)
	var result bson.M
	err := col.FindOne(ctx, bson.M{"_id": 1}).Decode(&result)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, nil
		}
		return nil, err
	}
	debug.Fprintf(os.Stderr, "segs: %v\n", result)
	res := result[ALL_SEGS].(primitive.A)
	s := make([]string, len(res))
	for i, v := range res {
		s[i] = v.(string)
	}
	return s, nil
}
