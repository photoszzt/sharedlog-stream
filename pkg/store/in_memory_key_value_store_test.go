package store

import (
	"context"
	"sharedlog-stream/pkg/treemap"
	"testing"
)

func getKeyValueStore() KeyValueStore {
	store := NewInMemoryBTreeKeyValueStore("test1", func(a, b treemap.Key) int {
		ka := a.(int)
		kb := b.(int)
		if ka < kb {
			return -1
		} else if ka == kb {
			return 0
		} else {
			return 1
		}
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
