package store

import (
	"context"
	"sharedlog-stream/pkg/concurrent_skiplist"
	"testing"
)

func getWindowStore(retainDuplicates bool) *InMemoryWindowStore {
	if !retainDuplicates {
		store := NewInMemoryWindowStore("test1", TEST_RETENTION_PERIOD, TEST_WINDOW_SIZE, retainDuplicates,
			concurrent_skiplist.CompareFunc(CompareNoDup))
		return store
	}
	store := NewInMemoryWindowStore("test1", TEST_RETENTION_PERIOD, TEST_WINDOW_SIZE, retainDuplicates,
		concurrent_skiplist.CompareFunc(CompareWithDup))
	return store
}

func TestGetAndRange(t *testing.T) {
	store := getWindowStore(false)
	GetAndRangeTest(store, t)
}

func TestShouldGetAllNonDeletedMsgs(t *testing.T) {
	store := getWindowStore(false)
	ShouldGetAllNonDeletedMsgsTest(store, t)
}

func TestExpiration(t *testing.T) {
	store := getWindowStore(false)
	ExpirationTest(store, t)
}

func TestShouldGetAll(t *testing.T) {
	store := getWindowStore(false)
	ShouldGetAllTest(store, t)
}

func TestShouldGetAllReturnTimestampOrdered(t *testing.T) {
	store := getWindowStore(false)
	ShouldGetAllReturnTimestampOrderedTest(store, t)
}

func TestFetchRange(t *testing.T) {
	store := getWindowStore(false)
	ctx := context.Background()
	FetchRangeTest(ctx, store, t)
}

func TestPutAndFetchBefore(t *testing.T) {
	store := getWindowStore(false)
	ctx := context.Background()
	PutAndFetchBeforeTest(ctx, store, t)
}

func TestPutAndFetchAfter(t *testing.T) {
	store := getWindowStore(false)
	ctx := context.Background()
	PutAndFetchAfterTest(ctx, store, t)
}

func TestPutSameKeyTs(t *testing.T) {
	store := getWindowStore(true)
	ctx := context.Background()
	PutSameKeyTsTest(ctx, store, t)
}
