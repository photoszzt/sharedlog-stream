package store

import (
	"context"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"testing"
)

func getMongoDBKeyValueStore(ctx context.Context, t *testing.T) *MongoDBKeyValueStore {
	store, err := NewMongoDBKeyValueStore(ctx, &MongoDBConfig{
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
	return store
}

func TestMongoDBShouldNotIncludeDeletedFromRangeResult(t *testing.T) {
	ctx := context.Background()
	store := getMongoDBKeyValueStore(ctx, t)
	store.client.Database("test").Drop(ctx)
	ShouldNotIncludeDeletedFromRangeResult(ctx, store, t)
	store.client.Database("test").Drop(ctx)
}

func TestMongoDBShouldDeleteIfSerializedValueIsNull(t *testing.T) {
	ctx := context.Background()
	store := getMongoDBKeyValueStore(ctx, t)
	store.client.Database("test").Drop(ctx)
	ShouldDeleteIfSerializedValueIsNull(ctx, store, t)
	store.client.Database("test").Drop(ctx)
}

func TestMongoDBPutGetRange(t *testing.T) {
	ctx := context.Background()
	store := getMongoDBKeyValueStore(ctx, t)
	store.client.Database("test").Drop(ctx)
	PutGetRange(ctx, store, t)
	store.client.Database("test").Drop(ctx)
}

func TestMongoDBRestore(t *testing.T) {
	ctx := context.Background()
	store := getMongoDBKeyValueStore(ctx, t)
	store.client.Database("test").Drop(ctx)
	if err := store.Put(ctx, 0, "zero"); err != nil {
		t.Fatal(err.Error())
	}
	if err := store.Put(ctx, 1, "one"); err != nil {
		t.Fatal(err.Error())
	}
	if err := store.Put(ctx, 2, "two"); err != nil {
		t.Fatal(err.Error())
	}
	if err := store.Put(ctx, 3, "three"); err != nil {
		t.Fatal(err.Error())
	}
	store2 := getMongoDBKeyValueStore(ctx, t)
	ret := make(map[int]string)
	store2.Range(ctx, nil, nil, func(kt commtypes.KeyT, vt commtypes.ValueT) error {
		ret[kt.(int)] = vt.(string)
		return nil
	})
	expected := map[int]string{
		0: "zero", 1: "one", 2: "two", 3: "three",
	}
	checkMapEqual(t, expected, ret)
	store.client.Database("test").Drop(ctx)
}
