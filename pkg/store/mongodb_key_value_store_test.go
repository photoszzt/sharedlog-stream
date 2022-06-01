package store

import (
	"context"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"testing"
)

func getMongoDBKeyValueStore(ctx context.Context, t *testing.T) *MongoDBKeyValueStore {
	client, err := InitMongoDBClient(ctx, "mongodb://localhost:27017")
	if err != nil {
		t.Fatal(err.Error())
	}
	store, err := NewMongoDBKeyValueStore(ctx, &MongoDBConfig{
		Client:         client,
		CollectionName: "a",
		KeySerde:       commtypes.IntSerde{},
		ValueSerde:     commtypes.StringSerde{},
		DBName:         "test",
	})
	if err != nil {
		t.Fatal(err.Error())
	}
	return store
}

func TestMongoDBShouldNotIncludeDeletedFromRangeResult(t *testing.T) {
	ctx := context.Background()
	store := getMongoDBKeyValueStore(ctx, t)
	checkErr(store.config.Client.Database("test").Drop(ctx), t)
	ShouldNotIncludeDeletedFromRangeResult(ctx, store, t)
	checkErr(store.config.Client.Database("test").Drop(ctx), t)
}

func TestMongoDBShouldDeleteIfSerializedValueIsNull(t *testing.T) {
	ctx := context.Background()
	store := getMongoDBKeyValueStore(ctx, t)
	checkErr(store.config.Client.Database("test").Drop(ctx), t)
	ShouldDeleteIfSerializedValueIsNull(ctx, store, t)
	checkErr(store.config.Client.Database("test").Drop(ctx), t)
}

func TestMongoDBPutGetRange(t *testing.T) {
	ctx := context.Background()
	store := getMongoDBKeyValueStore(ctx, t)
	checkErr(store.config.Client.Database("test").Drop(ctx), t)
	PutGetRange(ctx, store, t)
	checkErr(store.config.Client.Database("test").Drop(ctx), t)
}

func TestMongoDBRestore(t *testing.T) {
	ctx := context.Background()
	store := getMongoDBKeyValueStore(ctx, t)
	checkErr(store.config.Client.Database("test").Drop(ctx), t)
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
	checkErr(store2.Range(ctx, nil, nil, func(kt commtypes.KeyT, vt commtypes.ValueT) error {
		ret[kt.(int)] = vt.(string)
		return nil
	}), t)
	expected := map[int]string{
		0: "zero", 1: "one", 2: "two", 3: "three",
	}
	checkMapEqual(t, expected, ret)
	checkErr(store.config.Client.Database("test").Drop(ctx), t)
}
