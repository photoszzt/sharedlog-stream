package store

import (
	"context"
	"testing"
)

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
