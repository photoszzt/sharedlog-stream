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
