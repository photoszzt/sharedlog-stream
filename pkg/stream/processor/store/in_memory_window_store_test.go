package store

import (
	"context"
	"sharedlog-stream/pkg/concurrent_skiplist"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"testing"
)

func CompareNoDup(lhs, rhs interface{}) int {
	l := lhs.(uint32)
	r := rhs.(uint32)
	if l < r {
		return -1
	} else if l == r {
		return 0
	} else {
		return 1
	}
}

func CompareWithDup(lhs, rhs interface{}) int {
	ltmp := lhs.(VersionedKey)
	rtmp := rhs.(VersionedKey)
	l := ltmp.Key.(uint32)
	r := rtmp.Key.(uint32)
	if l < r {
		return -1
	} else if l == r {
		if ltmp.Version < rtmp.Version {
			return -1
		} else if ltmp.Version == rtmp.Version {
			return 0
		} else {
			return 1
		}
	} else {
		return 1
	}
}

func getWindowStore(retainDuplicates bool) *InMemoryWindowStore {
	if !retainDuplicates {
		store := NewInMemoryWindowStore("test1", RETENTION_PERIOD, WINDOW_SIZE, retainDuplicates,
			concurrent_skiplist.CompareFunc(CompareNoDup))
		return store
	}
	store := NewInMemoryWindowStore("test1", RETENTION_PERIOD, WINDOW_SIZE, retainDuplicates,
		concurrent_skiplist.CompareFunc(CompareWithDup))
	return store
}

func getWindowStoreWithChangelog(retainDuplicates bool) *InMemoryWindowStoreWithChangelog {
	mp := &MaterializeParam{
		KeySerde:    commtypes.Uint32Serde{},
		ValueSerde:  commtypes.StringSerde{},
		MsgSerde:    commtypes.MessageSerializedJSONSerde{},
		StoreName:   "test1",
		SerdeFormat: commtypes.JSON,
		ParNum:      0,
	}
	if !retainDuplicates {
		mp.Comparable = concurrent_skiplist.CompareFunc(CompareNoDup)
		store, err := NewInMemoryWindowStoreWithChangelog(RETENTION_PERIOD, WINDOW_SIZE,
			retainDuplicates, mp)
		if err != nil {
			panic(err)
		}
		return store
	}
	mp.Comparable = concurrent_skiplist.CompareFunc(CompareWithDup)
	store, err := NewInMemoryWindowStoreWithChangelog(RETENTION_PERIOD, WINDOW_SIZE,
		retainDuplicates, mp)
	if err != nil {
		panic(err)
	}
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
