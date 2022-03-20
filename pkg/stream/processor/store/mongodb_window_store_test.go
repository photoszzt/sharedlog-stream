package store

import (
	"context"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"testing"
	"time"
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
	byteStore, err := NewMongoDBSegmentedBytesStore(ctx, "test1",
		TEST_RETENTION_PERIOD, &WindowKeySchema{}, mkvs)
	if err != nil {
		t.Fatal(err.Error())
	}
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

func TestRolling(t *testing.T) {
	ctx := context.Background()
	store, mkvs := getMongoDBWindowStore(ctx, false, t)
	mkvs.client.Database("test").Drop(ctx)
	segmentInterval := TEST_RETENTION_PERIOD / 2
	if segmentInterval < 60_000 {
		segmentInterval = 60_000
	}
	segments, err := NewMongoDBKeyValueSegments(ctx, "test1", TEST_RETENTION_PERIOD,
		segmentInterval, mkvs)
	if err != nil {
		t.Fatal(err.Error())
	}
	RollingTest(ctx, store, segments, t)
	mkvs.client.Database("test").Drop(ctx)
}

func TestFetchDuplicates(t *testing.T) {
	ctx := context.Background()
	store, mkvs := getMongoDBWindowStore(ctx, true, t)
	mkvs.client.Database("test").Drop(ctx)
	FetchDuplicates(ctx, store, t)
	mkvs.client.Database("test").Drop(ctx)
}

func TestMongoDBWindowStoreRestore(t *testing.T) {
	ctx := context.Background()
	store, mkvs := getMongoDBWindowStore(ctx, false, t)
	mkvs.client.Database("test").Drop(ctx)
	if err := store.Put(ctx, uint32(1), "one", 0); err != nil {
		t.Fatal(err.Error())
	}
	if err := store.Put(ctx, uint32(2), "two", TEST_WINDOW_SIZE); err != nil {
		t.Fatal(err.Error())
	}
	if err := store.Put(ctx, uint32(3), "three", TEST_WINDOW_SIZE*2); err != nil {
		t.Fatal(err.Error())
	}
	store2, _ := getMongoDBWindowStore(ctx, false, t)
	ret := make(map[uint32]commtypes.ValueTimestamp)
	err := store2.FetchAll(ctx, time.UnixMilli(0), time.UnixMilli(TEST_WINDOW_SIZE*2),
		func(i int64, kt commtypes.KeyT, vt commtypes.ValueT) error {
			ret[kt.(uint32)] = commtypes.ValueTimestamp{Value: vt, Timestamp: i}
			return nil
		})
	if err != nil {
		t.Fatal(err.Error())
	}
	expected := map[uint32]commtypes.ValueTimestamp{
		1: {Value: "one", Timestamp: 0},
		2: {Value: "two", Timestamp: TEST_WINDOW_SIZE},
		3: {Value: "three", Timestamp: TEST_WINDOW_SIZE * 2},
	}
	checkKVTsMapEqual(t, expected, ret)
	mkvs.client.Database("test").Drop(ctx)
}

func checkKVTsMapEqual(t testing.TB, expected map[uint32]commtypes.ValueTimestamp, got map[uint32]commtypes.ValueTimestamp) {
	if len(expected) != len(got) {
		t.Fatalf("expected and got have different length. expected: %v, got: %v", expected, got)
	}
	for k, v := range expected {
		vgot := got[k]
		if vgot != v {
			t.Fatalf("k: %d, expected: %v, got: %v", k, v, vgot)
		}
	}
}
