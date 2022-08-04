package store

import (
	"context"
	"testing"
)

func getKeyValueStore() CoreKeyValueStore {
	store := NewInMemoryKeyValueStore("test1", IntLess)
	return store
}

func TestShouldNotIncludeDeletedFromRangeResult(t *testing.T) {
	ctx := context.Background()
	store := getKeyValueStore()
	ShouldNotIncludeDeletedFromRangeResult(ctx, store, t)
}

func TestShouldDeleteIfSerializedValueIsNull(t *testing.T) {
	ctx := context.Background()
	store := getKeyValueStore()
	ShouldDeleteIfSerializedValueIsNull(ctx, store, t)
}
