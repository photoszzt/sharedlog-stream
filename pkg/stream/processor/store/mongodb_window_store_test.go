package store

import (
	"context"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"testing"
)

func getMongoDBWindowStore(ctx context.Context, retainDuplicates bool, t *testing.T) (*SegmentedWindowStore, *MongoDBKeyValueStore) {
	mkvs, err := NewMongoDBKeyValueStore(ctx, &MongoDBConfig{
		Addr:           "mongodb://localhost:27017",
		CollectionName: "a",
		KeySerde:       commtypes.IntSerde{},
		ValueSerde:     commtypes.StringSerde{},
		DBName:         "test",
		StoreName:      "test1",
	})
	if err != nil {
		t.Fatal(err.Error())
	}
	byteStore := NewMongoDBSegmentedBytesStore("test1",
		TEST_RETENTION_PERIOD, &WindowKeySchema{}, mkvs)
	wstore := NewSegmentedWindowStore(byteStore, retainDuplicates, TEST_WINDOW_SIZE, commtypes.Uint32Serde{}, commtypes.StringSerde{})
	return wstore, mkvs
}

func TestMongoDBGetAndRange(t *testing.T) {
	ctx := context.Background()
	store, mkvs := getMongoDBWindowStore(ctx, false, t)
	mkvs.client.Database("test").Drop(ctx)
	GetAndRangeTest(ctx, store, t)
	mkvs.client.Database("test").Drop(ctx)
}

func TestMongoDBShouldGetAllNonDeletedMsgs(t *testing.T) {
	ctx := context.Background()
	store, mkvs := getMongoDBWindowStore(ctx, false, t)
	mkvs.client.Database("test").Drop(ctx)
	ShouldGetAllNonDeletedMsgsTest(ctx, store, t)
	mkvs.client.Database("test").Drop(ctx)
}

func TestMongoDBExpiration(t *testing.T) {
	ctx := context.Background()
	store, mkvs := getMongoDBWindowStore(ctx, false, t)
	mkvs.client.Database("test").Drop(ctx)
	ExpirationTest(ctx, store, t)
	mkvs.client.Database("test").Drop(ctx)
}
