package store

import (
	"context"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"testing"
)

func getRedisKeyValueStore() *RedisKeyValueStore {
	store := NewRedisKeyValueStore(&RedisConfig{
		Addr:           "127.0.0.1",
		Port:           6379,
		StoreName:      "test1",
		Userid:         0,
		CollectionName: "a",
		KeySerde:       commtypes.IntSerde{},
		ValueSerde:     commtypes.StringSerde{},
	})
	return store
}

func TestRedisShouldNotIncludeDeletedFromRangeResult(t *testing.T) {
	ctx := context.Background()
	store := getRedisKeyValueStore()
	ShouldNotIncludeDeletedFromRangeResult(ctx, store, t)
	store.rdb.FlushAll(ctx)
}

func TestRedisShouldDeleteIfSerializedValueIsNull(t *testing.T) {
	ctx := context.Background()
	store := getRedisKeyValueStore()
	ShouldDeleteIfSerializedValueIsNull(ctx, store, t)
	store.rdb.FlushAll(ctx)
}
