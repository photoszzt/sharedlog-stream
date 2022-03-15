package store

import (
	"context"
	"sharedlog-stream/pkg/stream/processor/commtypes"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	ALL_SEGS = "keys"
)

type MongoDBKeyValueSegment struct {
	mkvs        *MongoDBKeyValueStore
	segmentName string
	winName     string
}

func NewMongoDBKeyValueSegment(ctx context.Context,
	mkvs *MongoDBKeyValueStore,
	segmentName string,
	name string,
) (*MongoDBKeyValueSegment, error) {
	col := mkvs.client.Database(mkvs.config.DBName).Collection(name)
	opts := options.Update().SetUpsert(true)
	_, err := col.UpdateOne(ctx, bson.M{"_id": 1},
		bson.M{"$addToSet": bson.M{ALL_SEGS: segmentName}}, opts)
	if err != nil {
		return nil, err
	}
	// debug.Fprintf(os.Stderr, "insert return: %v\n", *ret)
	return &MongoDBKeyValueSegment{
		mkvs:        mkvs,
		segmentName: segmentName,
		winName:     name,
	}, nil
}

var _ = Segment(&MongoDBKeyValueSegment{})

func (mkvs *MongoDBKeyValueSegment) Init(ctx StoreContext) {}
func (mkvs *MongoDBKeyValueSegment) IsOpen() bool          { return true }
func (mkvs *MongoDBKeyValueSegment) Name() string          { return mkvs.segmentName }
func (mkvs *MongoDBKeyValueSegment) Get(ctx context.Context, key []byte) ([]byte, bool, error) {
	v, ok, err := mkvs.mkvs.GetWithCollection(ctx, key, mkvs.segmentName)
	return v, ok, err
}
func (mkvs *MongoDBKeyValueSegment) Range(ctx context.Context, from []byte, to []byte, iterFunc func([]byte, []byte) error) error {
	return mkvs.mkvs.RangeWithCollection(ctx, from, to, mkvs.segmentName, iterFunc)
}

func (mkvs *MongoDBKeyValueSegment) ReverseRange(from []byte, to []byte, iterFunc func([]byte, []byte) error) error {
	panic("not implemented")
}
func (mkvs *MongoDBKeyValueSegment) PrefixScan(prefix interface{}, prefixKeyEncoder commtypes.Encoder, iterFunc func([]byte, []byte) error) error {
	panic("not implemented")
}
func (mkvs *MongoDBKeyValueSegment) ApproximateNumEntries(ctx context.Context) (uint64, error) {
	panic("not implemented")
}
func (mkvs *MongoDBKeyValueSegment) Put(ctx context.Context, key []byte, value []byte) error {
	return mkvs.mkvs.PutWithCollection(ctx, key, value, mkvs.segmentName)
}

func (mkvs *MongoDBKeyValueSegment) PutIfAbsent(ctx context.Context, key []byte, value []byte) ([]byte, error) {
	panic("not implemented")
}
func (mkvs *MongoDBKeyValueSegment) PutAll(context.Context, []*commtypes.Message) error {
	panic("not implemented")
}
func (mkvs *MongoDBKeyValueSegment) Delete(ctx context.Context, key []byte) error {
	return mkvs.mkvs.DeleteWithCollection(ctx, key, mkvs.segmentName)
}

func (mkvs *MongoDBKeyValueSegment) Destroy(ctx context.Context) error {

	col_keys := mkvs.mkvs.client.Database(mkvs.mkvs.config.DBName).Collection(mkvs.winName)
	return col_keys.Drop(ctx)
}

func (mkvs *MongoDBKeyValueSegment) DeleteRange(ctx context.Context, keyFrom interface{}, keyTo interface{}) error {
	panic("not implemented")
}
