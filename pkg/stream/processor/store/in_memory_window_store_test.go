package store

import (
	"fmt"
	"sharedlog-stream/pkg/concurrent_skiplist"
	"testing"
	"time"
)

const (
	WINDOW_SIZE      = int64(3)
	SEGMENT_INTERVAL = int64(60_000)
	RETENTION_PERIOD = 2 * SEGMENT_INTERVAL
)

func getWindowStore() *InMemoryWindowStore {
	store := NewInMemoryWindowStore("test1", RETENTION_PERIOD, WINDOW_SIZE, false,
		concurrent_skiplist.CompareFunc(func(lhs, rhs interface{}) int {
			l := lhs.(uint32)
			r := rhs.(uint32)
			if l < r {
				return -1
			} else if l == r {
				return 0
			} else {
				return 1
			}
		}))
	return store
}

func putFirstBatch(store *InMemoryWindowStore, startTime int64) error {
	err := store.Put(uint32(0), "zero", startTime)
	if err != nil {
		return err
	}
	err = store.Put(uint32(1), "one", startTime+1)
	if err != nil {
		return err
	}
	err = store.Put(uint32(2), "two", startTime+2)
	if err != nil {
		return err
	}
	err = store.Put(uint32(4), "four", startTime+4)
	if err != nil {
		return err
	}
	err = store.Put(uint32(5), "five", startTime+5)
	return err
}

func putSecondBatch(store *InMemoryWindowStore, startTime int64) error {
	err := store.Put(uint32(2), "two+1", startTime+3)
	if err != nil {
		return err
	}
	err = store.Put(uint32(2), "two+2", startTime+4)
	if err != nil {
		return err
	}
	err = store.Put(uint32(2), "two+3", startTime+5)
	if err != nil {
		return err
	}
	err = store.Put(uint32(2), "two+4", startTime+6)
	if err != nil {
		return err
	}
	err = store.Put(uint32(2), "two+5", startTime+7)
	if err != nil {
		return err
	}
	err = store.Put(uint32(2), "two+6", startTime+8)
	return err
}

func assertGet(store *InMemoryWindowStore, k uint32, expected_val string, startTime int64) error {
	val, ok := store.Get(k, startTime)
	if !ok {
		return fmt.Errorf("key %d should exists\n", 0)
	}
	if val != expected_val {
		return fmt.Errorf("should be %s, but got %s", expected_val, val)
	}
	return nil
}

func assertFetch(store *InMemoryWindowStore, k uint32, timeFrom int64, timeTo int64) (map[string]struct{}, error) {
	res := make(map[string]struct{})
	err := store.Fetch(k, time.UnixMilli(timeFrom), time.UnixMilli(timeTo), func(i int64, vt ValueT) error {
		val := vt.(string)
		res[val] = struct{}{}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("fail to fetch: %v", err)
	}
	return res, nil
}

func TestGetAndRange(t *testing.T) {
	startTime := SEGMENT_INTERVAL - 4
	store := getWindowStore()
	err := putFirstBatch(store, startTime)
	if err != nil {
		t.Fatalf("fail to set up window store: %v\n", err)
	}

	k := uint32(0)
	expected_val := "zero"
	val, ok := store.Get(k, startTime)
	if !ok {
		t.Fatalf("key %d should exists\n", 0)
	}
	if val != expected_val {
		t.Fatalf("should be %s, but got %s", expected_val, val)
	}

	k = uint32(1)
	expected_val = "one"
	val, ok = store.Get(k, startTime+1)
	if !ok {
		t.Fatalf("key %d should exists\n", 0)
	}
	if val != expected_val {
		t.Fatalf("should be %s, but got %s", expected_val, val)
	}

	k = uint32(2)
	expected_val = "two"
	val, ok = store.Get(k, startTime+2)
	if !ok {
		t.Fatalf("key %d should exists\n", 0)
	}
	if val != expected_val {
		t.Fatalf("should be %s, but got %s", expected_val, val)
	}

	k = uint32(4)
	expected_val = "four"
	val, ok = store.Get(k, startTime+4)
	if !ok {
		t.Fatalf("key %d should exists\n", 0)
	}
	if val != expected_val {
		t.Fatalf("should be %s, but got %s", expected_val, val)
	}

	k = uint32(5)
	expected_val = "five"
	val, ok = store.Get(k, startTime+5)
	if !ok {
		t.Fatalf("key %d should exists\n", 0)
	}
	if val != expected_val {
		t.Fatalf("should be %s, but got %s", expected_val, val)
	}

	resM, err := assertFetch(store, 0, startTime-WINDOW_SIZE, startTime+WINDOW_SIZE)
	if err != nil {
		t.Fatalf("fail to fetch: %v", err)
	}
	expected := "zero"
	if _, ok := resM[expected]; !ok {
		t.Fatalf("expected list contains %s but got %v", expected, resM)
	}

	err = putSecondBatch(store, startTime)
	if err != nil {
		t.Fatalf("fail to put second batch")
	}

	k = uint32(2)
	expected_val = "two+1"
	val, ok = store.Get(k, startTime+3)
	if !ok {
		t.Fatalf("key %d should exists\n", 0)
	}
	if val != expected_val {
		t.Fatalf("should be %s, but got %s", expected_val, val)
	}

	err = assertGet(store, 2, "two+2", startTime+4)
	if err != nil {
		t.Fatalf("assertGet err: %v", err)
	}

	err = assertGet(store, 2, "two+3", startTime+5)
	if err != nil {
		t.Fatalf("assertGet err: %v", err)
	}

	err = assertGet(store, 2, "two+4", startTime+6)
	if err != nil {
		t.Fatalf("assertGet err: %v", err)
	}

	err = assertGet(store, 2, "two+5", startTime+7)
	if err != nil {
		t.Fatalf("assertGet err: %v", err)
	}

	err = assertGet(store, 2, "two+6", startTime+8)
	if err != nil {
		t.Fatalf("assertGet err: %v", err)
	}

	// TODO: fetch is broken
	resM, err = assertFetch(store, 2, startTime-2-WINDOW_SIZE, startTime-2+WINDOW_SIZE)
	if err != nil {
		t.Fatalf("assertFetch err: %v", err)
	}
	if len(resM) != 0 {
		t.Fatalf("expected empty list but got %v", resM)
	}

	resM, err = assertFetch(store, 2, startTime-1-WINDOW_SIZE, startTime-1+WINDOW_SIZE)
	if err != nil {
		t.Fatalf("assertFetch err: %v", err)
	}
	if _, ok := resM["two"]; !ok {
		t.Fatalf("expected list contains two but got %v", resM)
	}

	resM, err = assertFetch(store, 2, startTime-WINDOW_SIZE, startTime+WINDOW_SIZE)
	if err != nil {
		t.Fatalf("assertFetch err: %v", err)
	}
	expected = "two"
	if _, ok := resM[expected]; !ok {
		t.Fatalf("expected list contains %s but got %v", expected, resM)
	}
	expected = "two+1"
	if _, ok := resM[expected]; !ok {
		t.Fatalf("expected list contains %s but got %v", expected, resM)
	}

	resM, err = assertFetch(store, 2, startTime+1-WINDOW_SIZE, startTime+1+WINDOW_SIZE)
	if err != nil {
		t.Fatalf("assertFetch err: %v", err)
	}
	expected = "two"
	if _, ok := resM[expected]; !ok {
		t.Fatalf("expected list contains %s but got %v", expected, resM)
	}
	expected = "two+1"
	if _, ok := resM[expected]; !ok {
		t.Fatalf("expected list contains %s but got %v", expected, resM)
	}
	expected = "two+2"
	if _, ok := resM[expected]; !ok {
		t.Fatalf("expected list contains %s but got %v", expected, resM)
	}

	resM, err = assertFetch(store, 2, startTime+2-WINDOW_SIZE, startTime+2+WINDOW_SIZE)
	if err != nil {
		t.Fatalf("assertFetch err: %v", err)
	}
	expected = "two"
	if _, ok := resM[expected]; !ok {
		t.Fatalf("expected list contains %s but got %v", expected, resM)
	}
	expected = "two+1"
	if _, ok := resM[expected]; !ok {
		t.Fatalf("expected list contains %s but got %v", expected, resM)
	}
	expected = "two+2"
	if _, ok := resM[expected]; !ok {
		t.Fatalf("expected list contains %s but got %v", expected, resM)
	}
	expected = "two+3"
	if _, ok := resM[expected]; !ok {
		t.Fatalf("expected list contains %s but got %v", expected, resM)
	}

	resM, err = assertFetch(store, 2, startTime+3-WINDOW_SIZE, startTime+3+WINDOW_SIZE)
	if err != nil {
		t.Fatalf("assertFetch err: %v", err)
	}
	expected = "two"
	if _, ok := resM[expected]; !ok {
		t.Fatalf("expected list contains %s but got %v", expected, resM)
	}
	expected = "two+1"
	if _, ok := resM[expected]; !ok {
		t.Fatalf("expected list contains %s but got %v", expected, resM)
	}
	expected = "two+2"
	if _, ok := resM[expected]; !ok {
		t.Fatalf("expected list contains %s but got %v", expected, resM)
	}
	expected = "two+3"
	if _, ok := resM[expected]; !ok {
		t.Fatalf("expected list contains %s but got %v", expected, resM)
	}
	expected = "two+4"
	if _, ok := resM[expected]; !ok {
		t.Fatalf("expected list contains %s but got %v", expected, resM)
	}

	resM, err = assertFetch(store, 2, startTime+4-WINDOW_SIZE, startTime+4+WINDOW_SIZE)
	if err != nil {
		t.Fatalf("assertFetch err: %v", err)
	}
	expected = "two"
	if _, ok := resM[expected]; !ok {
		t.Fatalf("expected list contains %s but got %v", expected, resM)
	}
	expected = "two+1"
	if _, ok := resM[expected]; !ok {
		t.Fatalf("expected list contains %s but got %v", expected, resM)
	}
	expected = "two+2"
	if _, ok := resM[expected]; !ok {
		t.Fatalf("expected list contains %s but got %v", expected, resM)
	}
	expected = "two+3"
	if _, ok := resM[expected]; !ok {
		t.Fatalf("expected list contains %s but got %v", expected, resM)
	}
	expected = "two+4"
	if _, ok := resM[expected]; !ok {
		t.Fatalf("expected list contains %s but got %v", expected, resM)
	}
	expected = "two+5"
	if _, ok := resM[expected]; !ok {
		t.Fatalf("expected list contains %s but got %v", expected, resM)
	}

	resM, err = assertFetch(store, 2, startTime+5-WINDOW_SIZE, startTime+5+WINDOW_SIZE)
	if err != nil {
		t.Fatalf("assertFetch err: %v", err)
	}
	expected = "two"
	if _, ok := resM[expected]; !ok {
		t.Fatalf("expected list contains %s but got %v", expected, resM)
	}
	expected = "two+1"
	if _, ok := resM[expected]; !ok {
		t.Fatalf("expected list contains %s but got %v", expected, resM)
	}
	expected = "two+2"
	if _, ok := resM[expected]; !ok {
		t.Fatalf("expected list contains %s but got %v", expected, resM)
	}
	expected = "two+3"
	if _, ok := resM[expected]; !ok {
		t.Fatalf("expected list contains %s but got %v", expected, resM)
	}
	expected = "two+4"
	if _, ok := resM[expected]; !ok {
		t.Fatalf("expected list contains %s but got %v", expected, resM)
	}
	expected = "two+5"
	if _, ok := resM[expected]; !ok {
		t.Fatalf("expected list contains %s but got %v", expected, resM)
	}
	expected = "two+6"
	if _, ok := resM[expected]; !ok {
		t.Fatalf("expected list contains %s but got %v", expected, resM)
	}

	resM, err = assertFetch(store, 2, startTime+6-WINDOW_SIZE, startTime+6+WINDOW_SIZE)
	if err != nil {
		t.Fatalf("assertFetch err: %v", err)
	}
	expected = "two+1"
	if _, ok := resM[expected]; !ok {
		t.Fatalf("expected list contains %s but got %v", expected, resM)
	}
	expected = "two+2"
	if _, ok := resM[expected]; !ok {
		t.Fatalf("expected list contains %s but got %v", expected, resM)
	}
	expected = "two+3"
	if _, ok := resM[expected]; !ok {
		t.Fatalf("expected list contains %s but got %v", expected, resM)
	}
	expected = "two+4"
	if _, ok := resM[expected]; !ok {
		t.Fatalf("expected list contains %s but got %v", expected, resM)
	}
	expected = "two+5"
	if _, ok := resM[expected]; !ok {
		t.Fatalf("expected list contains %s but got %v", expected, resM)
	}
	expected = "two+6"
	if _, ok := resM[expected]; !ok {
		t.Fatalf("expected list contains %s but got %v", expected, resM)
	}

	resM, err = assertFetch(store, 2, startTime+7-WINDOW_SIZE, startTime+7+WINDOW_SIZE)
	if err != nil {
		t.Fatalf("assertFetch err: %v", err)
	}
	expected = "two+2"
	if _, ok := resM[expected]; !ok {
		t.Fatalf("expected list contains %s but got %v", expected, resM)
	}
	expected = "two+3"
	if _, ok := resM[expected]; !ok {
		t.Fatalf("expected list contains %s but got %v", expected, resM)
	}
	expected = "two+4"
	if _, ok := resM[expected]; !ok {
		t.Fatalf("expected list contains %s but got %v", expected, resM)
	}
	expected = "two+5"
	if _, ok := resM[expected]; !ok {
		t.Fatalf("expected list contains %s but got %v", expected, resM)
	}
	expected = "two+6"
	if _, ok := resM[expected]; !ok {
		t.Fatalf("expected list contains %s but got %v", expected, resM)
	}

	resM, err = assertFetch(store, 2, startTime+8-WINDOW_SIZE, startTime+8+WINDOW_SIZE)
	if err != nil {
		t.Fatalf("assertFetch err: %v", err)
	}
	expected = "two+3"
	if _, ok := resM[expected]; !ok {
		t.Fatalf("expected list contains %s but got %v", expected, resM)
	}
	expected = "two+4"
	if _, ok := resM[expected]; !ok {
		t.Fatalf("expected list contains %s but got %v", expected, resM)
	}
	expected = "two+5"
	if _, ok := resM[expected]; !ok {
		t.Fatalf("expected list contains %s but got %v", expected, resM)
	}
	expected = "two+6"
	if _, ok := resM[expected]; !ok {
		t.Fatalf("expected list contains %s but got %v", expected, resM)
	}

	resM, err = assertFetch(store, 2, startTime+9-WINDOW_SIZE, startTime+9+WINDOW_SIZE)
	if err != nil {
		t.Fatalf("assertFetch err: %v", err)
	}
	expected = "two+4"
	if _, ok := resM[expected]; !ok {
		t.Fatalf("expected list contains %s but got %v", expected, resM)
	}
	expected = "two+5"
	if _, ok := resM[expected]; !ok {
		t.Fatalf("expected list contains %s but got %v", expected, resM)
	}
	expected = "two+6"
	if _, ok := resM[expected]; !ok {
		t.Fatalf("expected list contains %s but got %v", expected, resM)
	}

	resM, err = assertFetch(store, 2, startTime+10-WINDOW_SIZE, startTime+10+WINDOW_SIZE)
	if err != nil {
		t.Fatalf("assertFetch err: %v", err)
	}
	expected = "two+5"
	if _, ok := resM[expected]; !ok {
		t.Fatalf("expected list contains %s but got %v", expected, resM)
	}
	expected = "two+6"
	if _, ok := resM[expected]; !ok {
		t.Fatalf("expected list contains %s but got %v", expected, resM)
	}

	resM, err = assertFetch(store, 2, startTime+11-WINDOW_SIZE, startTime+11+WINDOW_SIZE)
	if err != nil {
		t.Fatalf("assertFetch err: %v", err)
	}
	expected = "two+6"
	if _, ok := resM[expected]; !ok {
		t.Fatalf("expected list contains %s but got %v", expected, resM)
	}

	resM, err = assertFetch(store, 2, startTime+12-WINDOW_SIZE, startTime+12+WINDOW_SIZE)
	if err != nil {
		t.Fatalf("assertFetch err: %v", err)
	}
	if len(resM) != 0 {
		t.Fatalf("expected empty list but got %v", resM)
	}
}

func TestShouldGetAllNonDeletedMsgs(t *testing.T) {
	startTime := SEGMENT_INTERVAL - 4
	store := getWindowStore()

	err := store.Put(uint32(0), "zero", startTime)
	if err != nil {
		t.Fatalf("put err: %v", err)
	}

	err = store.Put(uint32(1), "one", startTime+1)
	if err != nil {
		t.Fatalf("put err: %v", err)
	}

	err = store.Put(uint32(2), "two", startTime+2)
	if err != nil {
		t.Fatalf("put err: %v", err)
	}

	err = store.Put(uint32(3), "three", startTime+3)
	if err != nil {
		t.Fatalf("put err: %v", err)
	}

	err = store.Put(uint32(4), "four", startTime+4)
	if err != nil {
		t.Fatalf("put err: %v", err)
	}

	err = store.Put(uint32(1), nil, startTime+1)
	if err != nil {
		t.Fatalf("put err: %v", err)
	}

	err = store.Put(uint32(3), nil, startTime+3)
	if err != nil {
		t.Fatalf("put err: %v", err)
	}

	k := uint32(0)
	expected := "zero"
	val, ok := store.Get(k, startTime)
	if !ok {
		t.Fatalf("expected key %d exists", 0)
	}
	if val != expected {
		t.Fatalf("got unexpected val: %s, expected %s", val, expected)
	}

	k = 2
	expected = "two"
	val, ok = store.Get(k, startTime+2)
	if !ok {
		t.Fatalf("expected key %d exists", 0)
	}
	if val != expected {
		t.Fatalf("got unexpected val: %s, expected %s", val, expected)
	}

	k = 4
	expected = "four"
	val, ok = store.Get(k, startTime+4)
	if err != nil {
		t.Fatalf("get err: %v", err)
	}
	if !ok {
		t.Fatalf("expected key %d exists", 0)
	}
	if val != expected {
		t.Fatalf("got unexpected val: %s, expected %s", val, expected)
	}

	val, _ = store.Get(uint32(1), startTime+1)
	if val != nil {
		t.Error("expected key 1 doesn't exist")
	}

	val, _ = store.Get(uint32(3), startTime+3)
	if val != nil {
		t.Error("expected key 3 doesn't exist")
	}
}

func TestExpiration(t *testing.T) {
	currentTime := int64(0)
	store := getWindowStore()
	err := store.Put(uint32(1), "one", int64(currentTime))
	if err != nil {
		t.Fatalf("put err: %v", err)
	}
	currentTime += RETENTION_PERIOD / 4
	err = store.Put(uint32(1), "two", int64(currentTime))
	if err != nil {
		t.Fatalf("put err: %v", err)
	}

	currentTime += RETENTION_PERIOD / 4
	err = store.Put(uint32(1), "three", int64(currentTime))
	if err != nil {
		t.Fatalf("put err: %v", err)
	}

	currentTime += RETENTION_PERIOD / 4
	err = store.Put(uint32(1), "four", int64(currentTime))
	if err != nil {
		t.Fatalf("put err: %v", err)
	}

	// increase current time to the full RETENTION_PERIOD to expire first record
	currentTime += RETENTION_PERIOD / 4
	err = store.Put(uint32(1), "five", int64(currentTime))
	if err != nil {
		t.Fatalf("put err: %v", err)
	}

	k := uint32(1)
	expected := "two"
	val, ok := store.Get(k, RETENTION_PERIOD/4)
	if !ok {
		t.Fatalf("expected key %d exists", 0)
	}
	if val != expected {
		t.Fatalf("got unexpected val: %s, expected %s", val, expected)
	}

	expected = "three"
	val, ok = store.Get(k, RETENTION_PERIOD/2)
	if !ok {
		t.Fatalf("expected key %d exists", 0)
	}
	if val != expected {
		t.Fatalf("got unexpected val: %s, expected %s", val, expected)
	}

	expected = "four"
	val, ok = store.Get(k, 3*RETENTION_PERIOD/4)
	if !ok {
		t.Fatalf("expected key %d exists", 0)
	}
	if val != expected {
		t.Fatalf("got unexpected val: %s, expected %s", val, expected)
	}

	expected = "five"
	val, ok = store.Get(k, RETENTION_PERIOD)
	if !ok {
		t.Fatalf("expected key %d exists", 0)
	}
	if val != expected {
		t.Fatalf("got unexpected val: %s, expected %s", val, expected)
	}

	currentTime += RETENTION_PERIOD / 4
	err = store.Put(uint32(1), "six", int64(currentTime))
	if err != nil {
		t.Fatalf("put err: %v", err)
	}

	expected = "three"
	val, ok = store.Get(k, RETENTION_PERIOD/2)
	if !ok {
		t.Fatalf("expected key %d exists", 0)
	}
	if val != expected {
		t.Fatalf("got unexpected val: %s, expected %s", val, expected)
	}

	expected = "four"
	val, ok = store.Get(k, 3*RETENTION_PERIOD/4)
	if !ok {
		t.Fatalf("expected key %d exists", 0)
	}
	if val != expected {
		t.Fatalf("got unexpected val: %s, expected %s", val, expected)
	}

	expected = "five"
	val, ok = store.Get(k, RETENTION_PERIOD)
	if !ok {
		t.Fatalf("expected key %d exists", 0)
	}
	if val != expected {
		t.Fatalf("got unexpected val: %s, expected %s", val, expected)
	}

	expected = "six"
	val, ok = store.Get(k, 5*RETENTION_PERIOD/4)
	if !ok {
		t.Fatalf("expected key %d exists", 0)
	}
	if val != expected {
		t.Fatalf("got unexpected val: %s, expected %s", val, expected)
	}
}
