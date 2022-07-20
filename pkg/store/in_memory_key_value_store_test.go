package store

import (
	"context"
	"testing"
)

func getKeyValueStore() KeyValueStore[int, string] {
	store := NewInMemoryBTreeKeyValueStore[int, string]("test1", func(a, b int) bool {
		return a < b
	})
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
