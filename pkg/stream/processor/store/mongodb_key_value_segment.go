package store

import (
	"context"
	"sharedlog-stream/pkg/stream/processor/commtypes"
)

type MongoDBKeyValueSegment struct {
	mkvs        *MongoDBKeyValueStore
	segmentName string
	name        string
}

func NewMongoDBKeyValueSegment(ctx context.Context,
	mkvs *MongoDBKeyValueStore,
	segmentName string,
	name string,
) *MongoDBKeyValueSegment {
	return &MongoDBKeyValueSegment{
		mkvs:        mkvs,
		segmentName: segmentName,
		name:        name,
	}
}

func (mkvs *MongoDBKeyValueSegment) Init(ctx StoreContext) {}
func (mkvs *MongoDBKeyValueSegment) IsOpen() bool          { return true }
func (mkvs *MongoDBKeyValueSegment) Name() string          { return mkvs.name }
func (mkvs *MongoDBKeyValueSegment) Get(ctx context.Context, key commtypes.KeyT) (commtypes.ValueT, bool, error) {
	return mkvs.mkvs.GetWithCollection(ctx, key, mkvs.segmentName)
}
func (mkvs *MongoDBKeyValueSegment) Range(ctx context.Context, from commtypes.KeyT, to commtypes.KeyT, iterFunc func(commtypes.KeyT, commtypes.ValueT) error) error {
	return mkvs.mkvs.Range(ctx, from, to, iterFunc)
}
func (mkvs *MongoDBKeyValueSegment) ReverseRange(from commtypes.KeyT, to commtypes.KeyT, iterFunc func(commtypes.KeyT, commtypes.ValueT) error) error {
	panic("not implemented")
}
func (mkvs *MongoDBKeyValueSegment) PrefixScan(prefix interface{}, prefixKeyEncoder commtypes.Encoder, iterFunc func(commtypes.KeyT, commtypes.ValueT) error) error {
	panic("not implemented")
}
func (mkvs *MongoDBKeyValueSegment) ApproximateNumEntries(ctx context.Context) (uint64, error) {
	panic("not implemented")
}
func (mkvs *MongoDBKeyValueSegment) Put(ctx context.Context, key commtypes.KeyT, value commtypes.ValueT) error {
	return mkvs.mkvs.PutWithCollection(ctx, key, value, mkvs.segmentName)
}

func (mkvs *MongoDBKeyValueSegment) PutIfAbsent(ctx context.Context, key commtypes.KeyT, value commtypes.ValueT) (commtypes.ValueT, error) {
	panic("not implemented")
}
func (mkvs *MongoDBKeyValueSegment) PutAll(context.Context, []*commtypes.Message) error {
	panic("not implemented")
}
func (mkvs *MongoDBKeyValueSegment) Delete(ctx context.Context, key commtypes.KeyT) error {
	return mkvs.mkvs.DeleteWithCollection(ctx, key, mkvs.segmentName)
}

func (mkvs *MongoDBKeyValueSegment) Destroy(ctx context.Context) error {
	panic("not implemented")
}

func (mkvs *MongoDBKeyValueSegment) DeleteRange(ctx context.Context, keyFrom interface{}, keyTo interface{}) error {
	panic("not implemented")
}
