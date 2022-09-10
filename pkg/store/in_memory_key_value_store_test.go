package store

import (
	"context"
	"testing"
)

/*
func getKeyValueStore() CoreKeyValueStore {
	store := NewInMemoryKeyValueStore("test1", IntLess)
	return store
}
*/

/*
func getBtreeKeyValueStoreG() CoreKeyValueStoreG[int, string] {
	store := NewInMemoryBTreeKeyValueStoreG[int, string]("test1", LessFunc[int](func(a, b int) bool {
		return a < b
	}))
	return store
}
*/

func getSkipmapKeyValueStoreG() CoreKeyValueStoreG[int, string] {
	store := NewInMemorySkipmapKeyValueStoreG[int, string]("test1", IntLessFunc)
	return store
}

// func getBtreeKeyValueStore() CoreKeyValueStore {
// 	store := NewInMemoryBTreeKeyValueStore("test1")
// 	return store
// }

func TestShouldNotIncludeDeletedFromRangeResult(t *testing.T) {
	ctx := context.Background()
	// store := getKeyValueStore()
	// ShouldNotIncludeDeletedFromRangeResult(ctx, store, t)
	// btreeStoreG := getBtreeKeyValueStoreG()
	// ShouldNotIncludeDeletedFromRangeResultG(ctx, btreeStoreG, t)
	// btreeStore := getBtreeKeyValueStore()
	// ShouldNotIncludeDeletedFromRangeResult(ctx, btreeStore, t)
	skipmapStoreG := getSkipmapKeyValueStoreG()
	ShouldNotIncludeDeletedFromRangeResultG(ctx, skipmapStoreG, t)
}

func TestShouldDeleteIfSerializedValueIsNull(t *testing.T) {
	ctx := context.Background()
	// store := getKeyValueStore()
	// ShouldDeleteIfSerializedValueIsNull(ctx, store, t)
	// btreeStore := getBtreeKeyValueStore()
	// ShouldDeleteIfSerializedValueIsNull(ctx, btreeStore, t)
	// btreeStoreG := getBtreeKeyValueStoreG()
	// ShouldDeleteIfSerializedValueIsNullG(ctx, btreeStoreG, t)
	skipmapStoreG := getSkipmapKeyValueStoreG()
	ShouldDeleteIfSerializedValueIsNullG(ctx, skipmapStoreG, t)
}
