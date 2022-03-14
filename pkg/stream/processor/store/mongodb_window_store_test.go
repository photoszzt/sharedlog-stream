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

/*
func TestMongoDBMongoDBShouldGetAll(t *testing.T) {
	ctx := context.Background()
	store, mkvs := getMongoDBWindowStore(ctx, false, t)
	mkvs.client.Database("test").Drop(ctx)
	ShouldGetAllTest(ctx, store, t)
	mkvs.client.Database("test").Drop(ctx)
}


func TestMongoDBShouldGetAllReturnTimestampOrdered(t *testing.T) {
	ctx := context.Background()
	store, mkvs := getMongoDBWindowStore(ctx, false, t)
	mkvs.client.Database("test").Drop(ctx)
	ShouldGetAllReturnTimestampOrderedTest(ctx, store, t)
	mkvs.client.Database("test").Drop(ctx)
}
*/

func TestMongoDBFetchRange(t *testing.T) {
	ctx := context.Background()
	store, mkvs := getMongoDBWindowStore(ctx, false, t)
	mkvs.client.Database("test").Drop(ctx)
	FetchRangeTest(ctx, store, t)
	mkvs.client.Database("test").Drop(ctx)
}

func TestMongoDBPutAndFetchBefore(t *testing.T) {
	ctx := context.Background()
	store, mkvs := getMongoDBWindowStore(ctx, false, t)
	mkvs.client.Database("test").Drop(ctx)
	PutAndFetchBeforeTest(ctx, store, t)
	mkvs.client.Database("test").Drop(ctx)
}

func TestMongoDBPutAndFetchAfter(t *testing.T) {
	ctx := context.Background()
	store, mkvs := getMongoDBWindowStore(ctx, false, t)
	mkvs.client.Database("test").Drop(ctx)
	PutAndFetchAfterTest(ctx, store, t)
	mkvs.client.Database("test").Drop(ctx)
}

func TestMongoDBPutSameKeyTs(t *testing.T) {
	ctx := context.Background()
	store, mkvs := getMongoDBWindowStore(ctx, true, t)
	mkvs.client.Database("test").Drop(ctx)
	PutSameKeyTsTest(ctx, store, t)
	mkvs.client.Database("test").Drop(ctx)
}

/*
func TestRolling(t *testing.T) {
	ctx := context.Background()
	store, mkvs := getMongoDBWindowStore(ctx, false, t)
	mkvs.client.Database("test").Drop(ctx)
	segmentInterval := TEST_RETENTION_PERIOD / 2
	if segmentInterval < 60_000 {
		segmentInterval = 60_000
	}
	segments := NewMongoDBKeyValueSegments("test1", TEST_RETENTION_PERIOD, segmentInterval, mkvs)
	RollingTest(ctx, store, segments, t)
	mkvs.client.Database("test").Drop(ctx)
}
*/
