package store

import (
	"fmt"
	"os"
	"sharedlog-stream/pkg/concurrent_skiplist"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"testing"
	"time"
)

const (
	WINDOW_SIZE      = int64(3)
	SEGMENT_INTERVAL = int64(60_000)
	RETENTION_PERIOD = 2 * SEGMENT_INTERVAL
)

func getWindowStore(retainDuplicates bool) *InMemoryWindowStore {
	if !retainDuplicates {
		store := NewInMemoryWindowStore("test1", RETENTION_PERIOD, WINDOW_SIZE, retainDuplicates,
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
	store := NewInMemoryWindowStore("test1", RETENTION_PERIOD, WINDOW_SIZE, retainDuplicates,
		concurrent_skiplist.CompareFunc(func(lhs, rhs interface{}) int {
			ltmp := lhs.(versionedKey)
			rtmp := rhs.(versionedKey)
			l := ltmp.key.(uint32)
			r := rtmp.key.(uint32)
			if l < r {
				return -1
			} else if l == r {
				if ltmp.version < rtmp.version {
					return -1
				} else if ltmp.version == rtmp.version {
					return 0
				} else {
					return 1
				}
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
	err := store.Fetch(k, time.UnixMilli(timeFrom), time.UnixMilli(timeTo), func(i int64, kt KeyT, vt ValueT) error {
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
	store := getWindowStore(false)
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
	store := getWindowStore(false)

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
	store := getWindowStore(false)
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

func TestShouldGetAll(t *testing.T) {
	startTime := SEGMENT_INTERVAL - 4
	store := getWindowStore(false)
	err := putFirstBatch(store, startTime)
	if err != nil {
		t.Fatalf("fail to set up window store: %v\n", err)
	}

	msgs := make([]commtypes.Message, 0)
	store.IterAll(func(ts int64, kt KeyT, vt ValueT) error {
		msg := commtypes.Message{
			Key:       kt,
			Value:     vt,
			Timestamp: ts,
		}
		msgs = append(msgs, msg)
		return nil
	})
	ref_msgs := []commtypes.Message{
		{
			Key:       uint32(0),
			Value:     "zero",
			Timestamp: startTime,
		},
		{
			Key:       uint32(1),
			Value:     "one",
			Timestamp: startTime + 1,
		},
		{
			Key:       uint32(2),
			Value:     "two",
			Timestamp: startTime + 2,
		},
		{
			Key:       uint32(4),
			Value:     "four",
			Timestamp: startTime + 4,
		},
		{
			Key:       uint32(5),
			Value:     "five",
			Timestamp: startTime + 5,
		},
	}
	checkSlice(ref_msgs, msgs, t)
}

func checkSlice(ref_msgs []commtypes.Message, msgs []commtypes.Message, t *testing.T) {
	if len(msgs) != len(ref_msgs) {
		t.Fatalf("store contains different number of elements. expected: %d, got %d", len(ref_msgs), len(msgs))
	}

	for idx, msg := range msgs {
		ref_msg := ref_msgs[idx]
		if msg.Key.(uint32) != ref_msg.Key {
			t.Fatalf("%dth item's key should be %v but got %v",
				idx, ref_msg.Key, msg.Key)
		}
		if msg.Value.(string) != ref_msg.Value {
			t.Fatalf("%dth item's val should be %v but got %v",
				idx, ref_msg.Value, msg.Value)
		}
		if msg.Timestamp != ref_msg.Timestamp {
			t.Fatalf("%dth item's ts should be %v but got %v",
				idx, ref_msg.Timestamp, msg.Timestamp)
		}
	}
}

func outOfOrderPut(store *InMemoryWindowStore, startTime int64) error {
	err := store.Put(uint32(4), "four", startTime+4)
	if err != nil {
		return err
	}
	err = store.Put(uint32(0), "zero", startTime)
	if err != nil {
		return err
	}
	err = store.Put(uint32(2), "two", startTime+2)
	if err != nil {
		return err
	}
	err = store.Put(uint32(3), "three", startTime+3)
	if err != nil {
		return err
	}
	err = store.Put(uint32(1), "one", startTime+1)
	if err != nil {
		return err
	}
	return nil
}

func TestShouldGetAllReturnTimestampOrdered(t *testing.T) {
	startTime := SEGMENT_INTERVAL - 4
	store := getWindowStore(false)
	err := outOfOrderPut(store, startTime)
	if err != nil {
		t.Fatalf("fail to setup entries in store: %v", err)
	}

	msgs := make([]commtypes.Message, 0)
	store.IterAll(func(ts int64, kt KeyT, vt ValueT) error {
		msg := commtypes.Message{
			Key:       kt,
			Value:     vt,
			Timestamp: ts,
		}
		msgs = append(msgs, msg)
		return nil
	})
	ref_msgs := []commtypes.Message{
		{
			Key:       uint32(0),
			Value:     "zero",
			Timestamp: startTime,
		},
		{
			Key:       uint32(1),
			Value:     "one",
			Timestamp: startTime + 1,
		},
		{
			Key:       uint32(2),
			Value:     "two",
			Timestamp: startTime + 2,
		},
		{
			Key:       uint32(3),
			Value:     "three",
			Timestamp: startTime + 3,
		},
		{
			Key:       uint32(4),
			Value:     "four",
			Timestamp: startTime + 4,
		},
	}
	checkSlice(ref_msgs, msgs, t)
}

func TestFetchRange(t *testing.T) {
	startTime := SEGMENT_INTERVAL - 4
	store := getWindowStore(false)
	err := putFirstBatch(store, startTime)
	if err != nil {
		t.Fatalf("fail to set up window store: %v\n", err)
	}

	fmt.Fprint(os.Stderr, "1\n")
	msgs := make([]commtypes.Message, 0)
	store.FetchWithKeyRange(uint32(0), uint32(1),
		time.UnixMilli(startTime-WINDOW_SIZE),
		time.UnixMilli(startTime+WINDOW_SIZE),
		func(ts int64, kt KeyT, vt ValueT) error {
			msg := commtypes.Message{
				Key:       kt,
				Value:     vt,
				Timestamp: ts,
			}
			msgs = append(msgs, msg)
			return nil
		})
	ref_msgs := []commtypes.Message{
		{
			Key:       uint32(0),
			Value:     "zero",
			Timestamp: startTime,
		},
		{
			Key:       uint32(1),
			Value:     "one",
			Timestamp: startTime + 1,
		},
	}
	checkSlice(ref_msgs, msgs, t)

	fmt.Fprint(os.Stderr, "2\n")
	msgs = make([]commtypes.Message, 0)
	store.FetchWithKeyRange(uint32(1), uint32(1),
		time.UnixMilli(startTime-WINDOW_SIZE),
		time.UnixMilli(startTime+WINDOW_SIZE),
		func(ts int64, kt KeyT, vt ValueT) error {
			msg := commtypes.Message{
				Key:       kt,
				Value:     vt,
				Timestamp: ts,
			}
			msgs = append(msgs, msg)
			return nil
		})
	ref_msgs = []commtypes.Message{
		{
			Key:       uint32(1),
			Value:     "one",
			Timestamp: startTime + 1,
		},
	}
	checkSlice(ref_msgs, msgs, t)

	fmt.Fprint(os.Stderr, "3\n")
	msgs = make([]commtypes.Message, 0)
	store.FetchWithKeyRange(uint32(1), uint32(3),
		time.UnixMilli(startTime-WINDOW_SIZE),
		time.UnixMilli(startTime+WINDOW_SIZE),
		func(ts int64, kt KeyT, vt ValueT) error {
			msg := commtypes.Message{
				Key:       kt,
				Value:     vt,
				Timestamp: ts,
			}
			msgs = append(msgs, msg)
			return nil
		})
	ref_msgs = []commtypes.Message{
		{
			Key:       uint32(1),
			Value:     "one",
			Timestamp: startTime + 1,
		},
		{
			Key:       uint32(2),
			Value:     "two",
			Timestamp: startTime + 2,
		},
	}
	checkSlice(ref_msgs, msgs, t)

	fmt.Fprint(os.Stderr, "4\n")
	msgs = make([]commtypes.Message, 0)
	store.FetchWithKeyRange(uint32(0), uint32(5),
		time.UnixMilli(startTime-WINDOW_SIZE),
		time.UnixMilli(startTime+WINDOW_SIZE),
		func(ts int64, kt KeyT, vt ValueT) error {
			msg := commtypes.Message{
				Key:       kt,
				Value:     vt,
				Timestamp: ts,
			}
			msgs = append(msgs, msg)
			return nil
		})
	ref_msgs = []commtypes.Message{
		{
			Key:       uint32(0),
			Value:     "zero",
			Timestamp: startTime,
		},
		{
			Key:       uint32(1),
			Value:     "one",
			Timestamp: startTime + 1,
		},
		{
			Key:       uint32(2),
			Value:     "two",
			Timestamp: startTime + 2,
		},
	}
	checkSlice(ref_msgs, msgs, t)

	fmt.Fprint(os.Stderr, "5\n")
	msgs = make([]commtypes.Message, 0)
	store.FetchWithKeyRange(uint32(0), uint32(5),
		time.UnixMilli(startTime-WINDOW_SIZE),
		time.UnixMilli(startTime+WINDOW_SIZE+5),
		func(ts int64, kt KeyT, vt ValueT) error {
			msg := commtypes.Message{
				Key:       kt,
				Value:     vt,
				Timestamp: ts,
			}
			msgs = append(msgs, msg)
			return nil
		})
	ref_msgs = []commtypes.Message{
		{
			Key:       uint32(0),
			Value:     "zero",
			Timestamp: startTime,
		},
		{
			Key:       uint32(1),
			Value:     "one",
			Timestamp: startTime + 1,
		},
		{
			Key:       uint32(2),
			Value:     "two",
			Timestamp: startTime + 2,
		},
		{
			Key:       uint32(4),
			Value:     "four",
			Timestamp: startTime + 4,
		},
		{
			Key:       uint32(5),
			Value:     "five",
			Timestamp: startTime + 5,
		},
	}
	checkSlice(ref_msgs, msgs, t)

	fmt.Fprint(os.Stderr, "6\n")
	msgs = make([]commtypes.Message, 0)
	store.FetchWithKeyRange(uint32(0), uint32(5),
		time.UnixMilli(startTime+2),
		time.UnixMilli(startTime+WINDOW_SIZE+5),
		func(ts int64, kt KeyT, vt ValueT) error {
			msg := commtypes.Message{
				Key:       kt,
				Value:     vt,
				Timestamp: ts,
			}
			msgs = append(msgs, msg)
			return nil
		})
	ref_msgs = []commtypes.Message{
		{
			Key:       uint32(2),
			Value:     "two",
			Timestamp: startTime + 2,
		},
		{
			Key:       uint32(4),
			Value:     "four",
			Timestamp: startTime + 4,
		},
		{
			Key:       uint32(5),
			Value:     "five",
			Timestamp: startTime + 5,
		},
	}
	checkSlice(ref_msgs, msgs, t)

	fmt.Fprint(os.Stderr, "7\n")
	msgs = make([]commtypes.Message, 0)
	store.FetchWithKeyRange(uint32(4), uint32(5),
		time.UnixMilli(startTime+2),
		time.UnixMilli(startTime+WINDOW_SIZE),
		func(ts int64, kt KeyT, vt ValueT) error {
			msg := commtypes.Message{
				Key:       kt,
				Value:     vt,
				Timestamp: ts,
			}
			msgs = append(msgs, msg)
			return nil
		})
	ref_msgs = make([]commtypes.Message, 0)
	checkSlice(ref_msgs, msgs, t)

	fmt.Fprint(os.Stderr, "8\n")
	msgs = make([]commtypes.Message, 0)
	store.FetchWithKeyRange(uint32(0), uint32(3),
		time.UnixMilli(startTime+3),
		time.UnixMilli(startTime+WINDOW_SIZE+5),
		func(ts int64, kt KeyT, vt ValueT) error {
			msg := commtypes.Message{
				Key:       kt,
				Value:     vt,
				Timestamp: ts,
			}
			msgs = append(msgs, msg)
			return nil
		})
	ref_msgs = make([]commtypes.Message, 0)
	checkSlice(ref_msgs, msgs, t)
}

func TestPutAndFetchBefore(t *testing.T) {
	startTime := SEGMENT_INTERVAL - 4
	store := getWindowStore(false)
	err := putFirstBatch(store, startTime)
	if err != nil {
		t.Fatalf("fail to set up window store: %v\n", err)
	}

	msgs := make([]commtypes.Message, 0)
	store.Fetch(uint32(0), time.UnixMilli(startTime-WINDOW_SIZE), time.UnixMilli(startTime), func(ts int64, kt KeyT, vt ValueT) error {
		msg := commtypes.Message{
			Key:       kt,
			Value:     vt,
			Timestamp: ts,
		}
		msgs = append(msgs, msg)
		return nil
	})
	ref_msgs := []commtypes.Message{
		{
			Key:       uint32(0),
			Value:     "zero",
			Timestamp: startTime,
		},
	}
	checkSlice(ref_msgs, msgs, t)

	msgs = make([]commtypes.Message, 0)
	store.Fetch(uint32(1), time.UnixMilli(startTime+1-WINDOW_SIZE), time.UnixMilli(startTime+1), func(ts int64, kt KeyT, vt ValueT) error {
		msg := commtypes.Message{
			Key:       kt,
			Value:     vt,
			Timestamp: ts,
		}
		msgs = append(msgs, msg)
		return nil
	})
	ref_msgs = []commtypes.Message{
		{
			Key:       uint32(1),
			Value:     "one",
			Timestamp: startTime + 1,
		},
	}
	checkSlice(ref_msgs, msgs, t)

	msgs = make([]commtypes.Message, 0)
	store.Fetch(uint32(2), time.UnixMilli(startTime+2-WINDOW_SIZE), time.UnixMilli(startTime+2), func(ts int64, kt KeyT, vt ValueT) error {
		msg := commtypes.Message{
			Key:       kt,
			Value:     vt,
			Timestamp: ts,
		}
		msgs = append(msgs, msg)
		return nil
	})
	ref_msgs = []commtypes.Message{
		{
			Key:       uint32(2),
			Value:     "two",
			Timestamp: startTime + 2,
		},
	}
	checkSlice(ref_msgs, msgs, t)

	msgs = make([]commtypes.Message, 0)
	store.Fetch(uint32(3), time.UnixMilli(startTime+3-WINDOW_SIZE), time.UnixMilli(startTime+3), func(ts int64, kt KeyT, vt ValueT) error {
		msg := commtypes.Message{
			Key:       kt,
			Value:     vt,
			Timestamp: ts,
		}
		msgs = append(msgs, msg)
		return nil
	})
	ref_msgs = make([]commtypes.Message, 0)
	checkSlice(ref_msgs, msgs, t)

	msgs = make([]commtypes.Message, 0)
	store.Fetch(uint32(4), time.UnixMilli(startTime+4-WINDOW_SIZE), time.UnixMilli(startTime+4), func(ts int64, kt KeyT, vt ValueT) error {
		msg := commtypes.Message{
			Key:       kt,
			Value:     vt,
			Timestamp: ts,
		}
		msgs = append(msgs, msg)
		return nil
	})
	ref_msgs = []commtypes.Message{
		{
			Key:       uint32(4),
			Value:     "four",
			Timestamp: startTime + 4,
		},
	}
	checkSlice(ref_msgs, msgs, t)

	msgs = make([]commtypes.Message, 0)
	store.Fetch(uint32(5), time.UnixMilli(startTime+5-WINDOW_SIZE), time.UnixMilli(startTime+5), func(ts int64, kt KeyT, vt ValueT) error {
		msg := commtypes.Message{
			Key:       kt,
			Value:     vt,
			Timestamp: ts,
		}
		msgs = append(msgs, msg)
		return nil
	})
	ref_msgs = []commtypes.Message{
		{
			Key:       uint32(5),
			Value:     "five",
			Timestamp: startTime + 5,
		},
	}
	checkSlice(ref_msgs, msgs, t)

	putSecondBatch(store, startTime)

	msgs = make([]commtypes.Message, 0)
	store.Fetch(uint32(2), time.UnixMilli(startTime-1-WINDOW_SIZE), time.UnixMilli(startTime-1), func(ts int64, kt KeyT, vt ValueT) error {
		msg := commtypes.Message{
			Key:       kt,
			Value:     vt,
			Timestamp: ts,
		}
		msgs = append(msgs, msg)
		return nil
	})
	ref_msgs = make([]commtypes.Message, 0)
	checkSlice(ref_msgs, msgs, t)

	msgs = make([]commtypes.Message, 0)
	store.Fetch(uint32(2), time.UnixMilli(startTime+0-WINDOW_SIZE), time.UnixMilli(startTime+0), func(ts int64, kt KeyT, vt ValueT) error {
		msg := commtypes.Message{
			Key:       kt,
			Value:     vt,
			Timestamp: ts,
		}
		msgs = append(msgs, msg)
		return nil
	})
	ref_msgs = make([]commtypes.Message, 0)
	checkSlice(ref_msgs, msgs, t)

	msgs = make([]commtypes.Message, 0)
	store.Fetch(uint32(2), time.UnixMilli(startTime+1-WINDOW_SIZE), time.UnixMilli(startTime+1), func(ts int64, kt KeyT, vt ValueT) error {
		msg := commtypes.Message{
			Key:       kt,
			Value:     vt,
			Timestamp: ts,
		}
		msgs = append(msgs, msg)
		return nil
	})
	ref_msgs = make([]commtypes.Message, 0)
	checkSlice(ref_msgs, msgs, t)

	msgs = make([]commtypes.Message, 0)
	store.Fetch(uint32(2), time.UnixMilli(startTime+2-WINDOW_SIZE), time.UnixMilli(startTime+2), func(ts int64, kt KeyT, vt ValueT) error {
		msg := commtypes.Message{
			Key:       kt,
			Value:     vt,
			Timestamp: ts,
		}
		msgs = append(msgs, msg)
		return nil
	})
	ref_msgs = []commtypes.Message{
		{
			Key:       uint32(2),
			Value:     "two",
			Timestamp: startTime + 2,
		},
	}
	checkSlice(ref_msgs, msgs, t)

	msgs = make([]commtypes.Message, 0)
	store.Fetch(uint32(2), time.UnixMilli(startTime+3-WINDOW_SIZE), time.UnixMilli(startTime+3), func(ts int64, kt KeyT, vt ValueT) error {
		msg := commtypes.Message{
			Key:       kt,
			Value:     vt,
			Timestamp: ts,
		}
		msgs = append(msgs, msg)
		return nil
	})
	ref_msgs = []commtypes.Message{
		{
			Key:       uint32(2),
			Value:     "two",
			Timestamp: startTime + 2,
		},
		{
			Key:       uint32(2),
			Value:     "two+1",
			Timestamp: startTime + 3,
		},
	}
	checkSlice(ref_msgs, msgs, t)

	msgs = make([]commtypes.Message, 0)
	store.Fetch(uint32(2), time.UnixMilli(startTime+4-WINDOW_SIZE), time.UnixMilli(startTime+4), func(ts int64, kt KeyT, vt ValueT) error {
		msg := commtypes.Message{
			Key:       kt,
			Value:     vt,
			Timestamp: ts,
		}
		msgs = append(msgs, msg)
		return nil
	})
	ref_msgs = []commtypes.Message{
		{
			Key:       uint32(2),
			Value:     "two",
			Timestamp: startTime + 2,
		},
		{
			Key:       uint32(2),
			Value:     "two+1",
			Timestamp: startTime + 3,
		},
		{
			Key:       uint32(2),
			Value:     "two+2",
			Timestamp: startTime + 4,
		},
	}
	checkSlice(ref_msgs, msgs, t)

	msgs = make([]commtypes.Message, 0)
	store.Fetch(uint32(2), time.UnixMilli(startTime+5-WINDOW_SIZE), time.UnixMilli(startTime+5), func(ts int64, kt KeyT, vt ValueT) error {
		msg := commtypes.Message{
			Key:       kt,
			Value:     vt,
			Timestamp: ts,
		}
		msgs = append(msgs, msg)
		return nil
	})
	ref_msgs = []commtypes.Message{
		{
			Key:       uint32(2),
			Value:     "two",
			Timestamp: startTime + 2,
		},
		{
			Key:       uint32(2),
			Value:     "two+1",
			Timestamp: startTime + 3,
		},
		{
			Key:       uint32(2),
			Value:     "two+2",
			Timestamp: startTime + 4,
		},
		{
			Key:       uint32(2),
			Value:     "two+3",
			Timestamp: startTime + 5,
		},
	}
	checkSlice(ref_msgs, msgs, t)

	msgs = make([]commtypes.Message, 0)
	store.Fetch(uint32(2), time.UnixMilli(startTime+6-WINDOW_SIZE), time.UnixMilli(startTime+6), func(ts int64, kt KeyT, vt ValueT) error {
		msg := commtypes.Message{
			Key:       kt,
			Value:     vt,
			Timestamp: ts,
		}
		msgs = append(msgs, msg)
		return nil
	})
	ref_msgs = []commtypes.Message{
		{
			Key:       uint32(2),
			Value:     "two+1",
			Timestamp: startTime + 3,
		},
		{
			Key:       uint32(2),
			Value:     "two+2",
			Timestamp: startTime + 4,
		},
		{
			Key:       uint32(2),
			Value:     "two+3",
			Timestamp: startTime + 5,
		},
		{
			Key:       uint32(2),
			Value:     "two+4",
			Timestamp: startTime + 6,
		},
	}
	checkSlice(ref_msgs, msgs, t)

	msgs = make([]commtypes.Message, 0)
	store.Fetch(uint32(2), time.UnixMilli(startTime+7-WINDOW_SIZE), time.UnixMilli(startTime+7), func(ts int64, kt KeyT, vt ValueT) error {
		msg := commtypes.Message{
			Key:       kt,
			Value:     vt,
			Timestamp: ts,
		}
		msgs = append(msgs, msg)
		return nil
	})
	ref_msgs = []commtypes.Message{
		{
			Key:       uint32(2),
			Value:     "two+2",
			Timestamp: startTime + 4,
		},
		{
			Key:       uint32(2),
			Value:     "two+3",
			Timestamp: startTime + 5,
		},
		{
			Key:       uint32(2),
			Value:     "two+4",
			Timestamp: startTime + 6,
		},
		{
			Key:       uint32(2),
			Value:     "two+5",
			Timestamp: startTime + 7,
		},
	}
	checkSlice(ref_msgs, msgs, t)

	msgs = make([]commtypes.Message, 0)
	store.Fetch(uint32(2), time.UnixMilli(startTime+8-WINDOW_SIZE), time.UnixMilli(startTime+8), func(ts int64, kt KeyT, vt ValueT) error {
		msg := commtypes.Message{
			Key:       kt,
			Value:     vt,
			Timestamp: ts,
		}
		msgs = append(msgs, msg)
		return nil
	})
	ref_msgs = []commtypes.Message{
		{
			Key:       uint32(2),
			Value:     "two+3",
			Timestamp: startTime + 5,
		},
		{
			Key:       uint32(2),
			Value:     "two+4",
			Timestamp: startTime + 6,
		},
		{
			Key:       uint32(2),
			Value:     "two+5",
			Timestamp: startTime + 7,
		},
		{
			Key:       uint32(2),
			Value:     "two+6",
			Timestamp: startTime + 8,
		},
	}
	checkSlice(ref_msgs, msgs, t)

	msgs = make([]commtypes.Message, 0)
	store.Fetch(uint32(2), time.UnixMilli(startTime+9-WINDOW_SIZE), time.UnixMilli(startTime+9), func(ts int64, kt KeyT, vt ValueT) error {
		msg := commtypes.Message{
			Key:       kt,
			Value:     vt,
			Timestamp: ts,
		}
		msgs = append(msgs, msg)
		return nil
	})
	ref_msgs = []commtypes.Message{
		{
			Key:       uint32(2),
			Value:     "two+4",
			Timestamp: startTime + 6,
		},
		{
			Key:       uint32(2),
			Value:     "two+5",
			Timestamp: startTime + 7,
		},
		{
			Key:       uint32(2),
			Value:     "two+6",
			Timestamp: startTime + 8,
		},
	}
	checkSlice(ref_msgs, msgs, t)

	msgs = make([]commtypes.Message, 0)
	store.Fetch(uint32(2), time.UnixMilli(startTime+10-WINDOW_SIZE), time.UnixMilli(startTime+10), func(ts int64, kt KeyT, vt ValueT) error {
		msg := commtypes.Message{
			Key:       kt,
			Value:     vt,
			Timestamp: ts,
		}
		msgs = append(msgs, msg)
		return nil
	})
	ref_msgs = []commtypes.Message{
		{
			Key:       uint32(2),
			Value:     "two+5",
			Timestamp: startTime + 7,
		},
		{
			Key:       uint32(2),
			Value:     "two+6",
			Timestamp: startTime + 8,
		},
	}
	checkSlice(ref_msgs, msgs, t)

	msgs = make([]commtypes.Message, 0)
	store.Fetch(uint32(2), time.UnixMilli(startTime+11-WINDOW_SIZE), time.UnixMilli(startTime+11), func(ts int64, kt KeyT, vt ValueT) error {
		msg := commtypes.Message{
			Key:       kt,
			Value:     vt,
			Timestamp: ts,
		}
		msgs = append(msgs, msg)
		return nil
	})
	ref_msgs = []commtypes.Message{
		{
			Key:       uint32(2),
			Value:     "two+6",
			Timestamp: startTime + 8,
		},
	}
	checkSlice(ref_msgs, msgs, t)

	msgs = make([]commtypes.Message, 0)
	store.Fetch(uint32(2), time.UnixMilli(startTime+12-WINDOW_SIZE), time.UnixMilli(startTime+12), func(ts int64, kt KeyT, vt ValueT) error {
		msg := commtypes.Message{
			Key:       kt,
			Value:     vt,
			Timestamp: ts,
		}
		msgs = append(msgs, msg)
		return nil
	})
	ref_msgs = make([]commtypes.Message, 0)
	checkSlice(ref_msgs, msgs, t)

	msgs = make([]commtypes.Message, 0)
	store.Fetch(uint32(2), time.UnixMilli(startTime+13-WINDOW_SIZE), time.UnixMilli(startTime+13), func(ts int64, kt KeyT, vt ValueT) error {
		msg := commtypes.Message{
			Key:       kt,
			Value:     vt,
			Timestamp: ts,
		}
		msgs = append(msgs, msg)
		return nil
	})
	ref_msgs = make([]commtypes.Message, 0)
	checkSlice(ref_msgs, msgs, t)
}

func TestPutAndFetchAfter(t *testing.T) {
	startTime := SEGMENT_INTERVAL - 4
	store := getWindowStore(false)
	err := putFirstBatch(store, startTime)
	if err != nil {
		t.Fatalf("fail to set up window store: %v\n", err)
	}

	fmt.Fprint(os.Stderr, "11\n")
	msgs := make([]commtypes.Message, 0)
	store.Fetch(uint32(0), time.UnixMilli(startTime), time.UnixMilli(startTime+WINDOW_SIZE), func(ts int64, kt KeyT, vt ValueT) error {
		msg := commtypes.Message{
			Key:       kt,
			Value:     vt,
			Timestamp: ts,
		}
		msgs = append(msgs, msg)
		return nil
	})
	ref_msgs := []commtypes.Message{
		{
			Key:       uint32(0),
			Value:     "zero",
			Timestamp: startTime,
		},
	}
	checkSlice(ref_msgs, msgs, t)

	fmt.Fprint(os.Stderr, "12\n")
	msgs = make([]commtypes.Message, 0)
	store.Fetch(uint32(1), time.UnixMilli(startTime+1), time.UnixMilli(startTime+1+WINDOW_SIZE), func(ts int64, kt KeyT, vt ValueT) error {
		msg := commtypes.Message{
			Key:       kt,
			Value:     vt,
			Timestamp: ts,
		}
		msgs = append(msgs, msg)
		return nil
	})
	ref_msgs = []commtypes.Message{
		{
			Key:       uint32(1),
			Value:     "one",
			Timestamp: startTime + 1,
		},
	}
	checkSlice(ref_msgs, msgs, t)

	fmt.Fprint(os.Stderr, "13\n")
	msgs = make([]commtypes.Message, 0)
	store.Fetch(uint32(2), time.UnixMilli(startTime+2), time.UnixMilli(startTime+2+WINDOW_SIZE), func(ts int64, kt KeyT, vt ValueT) error {
		msg := commtypes.Message{
			Key:       kt,
			Value:     vt,
			Timestamp: ts,
		}
		msgs = append(msgs, msg)
		return nil
	})
	ref_msgs = []commtypes.Message{
		{
			Key:       uint32(2),
			Value:     "two",
			Timestamp: startTime + 2,
		},
	}
	checkSlice(ref_msgs, msgs, t)

	fmt.Fprint(os.Stderr, "14\n")
	msgs = make([]commtypes.Message, 0)
	store.Fetch(uint32(3), time.UnixMilli(startTime+3), time.UnixMilli(startTime+3+WINDOW_SIZE), func(ts int64, kt KeyT, vt ValueT) error {
		msg := commtypes.Message{
			Key:       kt,
			Value:     vt,
			Timestamp: ts,
		}
		msgs = append(msgs, msg)
		return nil
	})
	ref_msgs = make([]commtypes.Message, 0)
	checkSlice(ref_msgs, msgs, t)

	fmt.Fprint(os.Stderr, "15\n")
	msgs = make([]commtypes.Message, 0)
	store.Fetch(uint32(4), time.UnixMilli(startTime+4), time.UnixMilli(startTime+4+WINDOW_SIZE), func(ts int64, kt KeyT, vt ValueT) error {
		msg := commtypes.Message{
			Key:       kt,
			Value:     vt,
			Timestamp: ts,
		}
		msgs = append(msgs, msg)
		return nil
	})
	ref_msgs = []commtypes.Message{
		{
			Key:       uint32(4),
			Value:     "four",
			Timestamp: startTime + 4,
		},
	}
	checkSlice(ref_msgs, msgs, t)

	fmt.Fprint(os.Stderr, "16\n")
	msgs = make([]commtypes.Message, 0)
	store.Fetch(uint32(5), time.UnixMilli(startTime+5), time.UnixMilli(startTime+5+WINDOW_SIZE), func(ts int64, kt KeyT, vt ValueT) error {
		msg := commtypes.Message{
			Key:       kt,
			Value:     vt,
			Timestamp: ts,
		}
		msgs = append(msgs, msg)
		return nil
	})
	ref_msgs = []commtypes.Message{
		{
			Key:       uint32(5),
			Value:     "five",
			Timestamp: startTime + 5,
		},
	}
	checkSlice(ref_msgs, msgs, t)

	putSecondBatch(store, startTime)

	fmt.Fprint(os.Stderr, "21\n")
	msgs = make([]commtypes.Message, 0)
	store.Fetch(uint32(2), time.UnixMilli(startTime-2), time.UnixMilli(startTime-2+WINDOW_SIZE), func(ts int64, kt KeyT, vt ValueT) error {
		msg := commtypes.Message{
			Key:       kt,
			Value:     vt,
			Timestamp: ts,
		}
		msgs = append(msgs, msg)
		return nil
	})
	ref_msgs = make([]commtypes.Message, 0)
	checkSlice(ref_msgs, msgs, t)

	fmt.Fprint(os.Stderr, "22\n")
	msgs = make([]commtypes.Message, 0)
	store.Fetch(uint32(2), time.UnixMilli(startTime-1), time.UnixMilli(startTime-1+WINDOW_SIZE), func(ts int64, kt KeyT, vt ValueT) error {
		msg := commtypes.Message{
			Key:       kt,
			Value:     vt,
			Timestamp: ts,
		}
		msgs = append(msgs, msg)
		return nil
	})
	ref_msgs = []commtypes.Message{
		{
			Key:       uint32(2),
			Value:     "two",
			Timestamp: startTime + 2,
		},
	}
	checkSlice(ref_msgs, msgs, t)

	fmt.Fprint(os.Stderr, "23\n")
	msgs = make([]commtypes.Message, 0)
	store.Fetch(uint32(2), time.UnixMilli(startTime), time.UnixMilli(startTime+WINDOW_SIZE), func(ts int64, kt KeyT, vt ValueT) error {
		msg := commtypes.Message{
			Key:       kt,
			Value:     vt,
			Timestamp: ts,
		}
		msgs = append(msgs, msg)
		return nil
	})
	ref_msgs = []commtypes.Message{
		{
			Key:       uint32(2),
			Value:     "two",
			Timestamp: startTime + 2,
		},
		{
			Key:       uint32(2),
			Value:     "two+1",
			Timestamp: startTime + 3,
		},
	}
	checkSlice(ref_msgs, msgs, t)

	fmt.Fprint(os.Stderr, "24\n")
	msgs = make([]commtypes.Message, 0)
	store.Fetch(uint32(2), time.UnixMilli(startTime+1), time.UnixMilli(startTime+1+WINDOW_SIZE), func(ts int64, kt KeyT, vt ValueT) error {
		msg := commtypes.Message{
			Key:       kt,
			Value:     vt,
			Timestamp: ts,
		}
		msgs = append(msgs, msg)
		return nil
	})
	ref_msgs = []commtypes.Message{
		{
			Key:       uint32(2),
			Value:     "two",
			Timestamp: startTime + 2,
		},
		{
			Key:       uint32(2),
			Value:     "two+1",
			Timestamp: startTime + 3,
		},
		{
			Key:       uint32(2),
			Value:     "two+2",
			Timestamp: startTime + 4,
		},
	}
	checkSlice(ref_msgs, msgs, t)

	fmt.Fprint(os.Stderr, "25\n")
	msgs = make([]commtypes.Message, 0)
	store.Fetch(uint32(2), time.UnixMilli(startTime+2), time.UnixMilli(startTime+2+WINDOW_SIZE), func(ts int64, kt KeyT, vt ValueT) error {
		msg := commtypes.Message{
			Key:       kt,
			Value:     vt,
			Timestamp: ts,
		}
		msgs = append(msgs, msg)
		return nil
	})
	ref_msgs = []commtypes.Message{
		{
			Key:       uint32(2),
			Value:     "two",
			Timestamp: startTime + 2,
		},
		{
			Key:       uint32(2),
			Value:     "two+1",
			Timestamp: startTime + 3,
		},
		{
			Key:       uint32(2),
			Value:     "two+2",
			Timestamp: startTime + 4,
		},
		{
			Key:       uint32(2),
			Value:     "two+3",
			Timestamp: startTime + 5,
		},
	}
	checkSlice(ref_msgs, msgs, t)

	fmt.Fprint(os.Stderr, "26\n")
	msgs = make([]commtypes.Message, 0)
	store.Fetch(uint32(2), time.UnixMilli(startTime+3), time.UnixMilli(startTime+3+WINDOW_SIZE), func(ts int64, kt KeyT, vt ValueT) error {
		msg := commtypes.Message{
			Key:       kt,
			Value:     vt,
			Timestamp: ts,
		}
		msgs = append(msgs, msg)
		return nil
	})
	ref_msgs = []commtypes.Message{
		{
			Key:       uint32(2),
			Value:     "two+1",
			Timestamp: startTime + 3,
		},
		{
			Key:       uint32(2),
			Value:     "two+2",
			Timestamp: startTime + 4,
		},
		{
			Key:       uint32(2),
			Value:     "two+3",
			Timestamp: startTime + 5,
		},
		{
			Key:       uint32(2),
			Value:     "two+4",
			Timestamp: startTime + 6,
		},
	}
	checkSlice(ref_msgs, msgs, t)

	fmt.Fprint(os.Stderr, "27\n")
	msgs = make([]commtypes.Message, 0)
	store.Fetch(uint32(2), time.UnixMilli(startTime+4), time.UnixMilli(startTime+4+WINDOW_SIZE), func(ts int64, kt KeyT, vt ValueT) error {
		msg := commtypes.Message{
			Key:       kt,
			Value:     vt,
			Timestamp: ts,
		}
		msgs = append(msgs, msg)
		return nil
	})
	ref_msgs = []commtypes.Message{
		{
			Key:       uint32(2),
			Value:     "two+2",
			Timestamp: startTime + 4,
		},
		{
			Key:       uint32(2),
			Value:     "two+3",
			Timestamp: startTime + 5,
		},
		{
			Key:       uint32(2),
			Value:     "two+4",
			Timestamp: startTime + 6,
		},
		{
			Key:       uint32(2),
			Value:     "two+5",
			Timestamp: startTime + 7,
		},
	}
	checkSlice(ref_msgs, msgs, t)

	fmt.Fprint(os.Stderr, "28\n")
	msgs = make([]commtypes.Message, 0)
	store.Fetch(uint32(2), time.UnixMilli(startTime+5), time.UnixMilli(startTime+5+WINDOW_SIZE), func(ts int64, kt KeyT, vt ValueT) error {
		msg := commtypes.Message{
			Key:       kt,
			Value:     vt,
			Timestamp: ts,
		}
		msgs = append(msgs, msg)
		return nil
	})
	ref_msgs = []commtypes.Message{
		{
			Key:       uint32(2),
			Value:     "two+3",
			Timestamp: startTime + 5,
		},
		{
			Key:       uint32(2),
			Value:     "two+4",
			Timestamp: startTime + 6,
		},
		{
			Key:       uint32(2),
			Value:     "two+5",
			Timestamp: startTime + 7,
		},
		{
			Key:       uint32(2),
			Value:     "two+6",
			Timestamp: startTime + 8,
		},
	}
	checkSlice(ref_msgs, msgs, t)

	fmt.Fprint(os.Stderr, "29\n")
	msgs = make([]commtypes.Message, 0)
	store.Fetch(uint32(2), time.UnixMilli(startTime+6), time.UnixMilli(startTime+6+WINDOW_SIZE), func(ts int64, kt KeyT, vt ValueT) error {
		msg := commtypes.Message{
			Key:       kt,
			Value:     vt,
			Timestamp: ts,
		}
		msgs = append(msgs, msg)
		return nil
	})
	ref_msgs = []commtypes.Message{
		{
			Key:       uint32(2),
			Value:     "two+4",
			Timestamp: startTime + 6,
		},
		{
			Key:       uint32(2),
			Value:     "two+5",
			Timestamp: startTime + 7,
		},
		{
			Key:       uint32(2),
			Value:     "two+6",
			Timestamp: startTime + 8,
		},
	}
	checkSlice(ref_msgs, msgs, t)

	fmt.Fprint(os.Stderr, "30\n")
	msgs = make([]commtypes.Message, 0)
	store.Fetch(uint32(2), time.UnixMilli(startTime+7), time.UnixMilli(startTime+7+WINDOW_SIZE), func(ts int64, kt KeyT, vt ValueT) error {
		msg := commtypes.Message{
			Key:       kt,
			Value:     vt,
			Timestamp: ts,
		}
		msgs = append(msgs, msg)
		return nil
	})
	ref_msgs = []commtypes.Message{
		{
			Key:       uint32(2),
			Value:     "two+5",
			Timestamp: startTime + 7,
		},
		{
			Key:       uint32(2),
			Value:     "two+6",
			Timestamp: startTime + 8,
		},
	}
	checkSlice(ref_msgs, msgs, t)

	fmt.Fprint(os.Stderr, "31\n")
	msgs = make([]commtypes.Message, 0)
	store.Fetch(uint32(2), time.UnixMilli(startTime+8), time.UnixMilli(startTime+8+WINDOW_SIZE), func(ts int64, kt KeyT, vt ValueT) error {
		msg := commtypes.Message{
			Key:       kt,
			Value:     vt,
			Timestamp: ts,
		}
		msgs = append(msgs, msg)
		return nil
	})
	ref_msgs = []commtypes.Message{
		{
			Key:       uint32(2),
			Value:     "two+6",
			Timestamp: startTime + 8,
		},
	}
	checkSlice(ref_msgs, msgs, t)

	fmt.Fprint(os.Stderr, "32\n")
	msgs = make([]commtypes.Message, 0)
	store.Fetch(uint32(2), time.UnixMilli(startTime+9), time.UnixMilli(startTime+9+WINDOW_SIZE), func(ts int64, kt KeyT, vt ValueT) error {
		msg := commtypes.Message{
			Key:       kt,
			Value:     vt,
			Timestamp: ts,
		}
		msgs = append(msgs, msg)
		return nil
	})
	ref_msgs = make([]commtypes.Message, 0)
	checkSlice(ref_msgs, msgs, t)

	fmt.Fprint(os.Stderr, "33\n")
	msgs = make([]commtypes.Message, 0)
	store.Fetch(uint32(2), time.UnixMilli(startTime+10), time.UnixMilli(startTime+10+WINDOW_SIZE), func(ts int64, kt KeyT, vt ValueT) error {
		msg := commtypes.Message{
			Key:       kt,
			Value:     vt,
			Timestamp: ts,
		}
		msgs = append(msgs, msg)
		return nil
	})
	ref_msgs = make([]commtypes.Message, 0)
	checkSlice(ref_msgs, msgs, t)

	fmt.Fprint(os.Stderr, "34\n")
	msgs = make([]commtypes.Message, 0)
	store.Fetch(uint32(2), time.UnixMilli(startTime+11), time.UnixMilli(startTime+11+WINDOW_SIZE), func(ts int64, kt KeyT, vt ValueT) error {
		msg := commtypes.Message{
			Key:       kt,
			Value:     vt,
			Timestamp: ts,
		}
		msgs = append(msgs, msg)
		return nil
	})
	ref_msgs = make([]commtypes.Message, 0)
	checkSlice(ref_msgs, msgs, t)

	fmt.Fprint(os.Stderr, "35\n")
	msgs = make([]commtypes.Message, 0)
	store.Fetch(uint32(2), time.UnixMilli(startTime+12), time.UnixMilli(startTime+12+WINDOW_SIZE), func(ts int64, kt KeyT, vt ValueT) error {
		msg := commtypes.Message{
			Key:       kt,
			Value:     vt,
			Timestamp: ts,
		}
		msgs = append(msgs, msg)
		return nil
	})
	ref_msgs = make([]commtypes.Message, 0)
	checkSlice(ref_msgs, msgs, t)
}

func TestPutSameKeyTs(t *testing.T) {
	startTime := SEGMENT_INTERVAL - 4
	store := getWindowStore(true)

	err := store.Put(uint32(0), "zero", startTime)
	if err != nil {
		t.Fatalf("fail to put err: %v", err)
	}

	msgs := make([]commtypes.Message, 0)
	store.Fetch(uint32(0), time.UnixMilli(startTime-WINDOW_SIZE), time.UnixMilli(startTime+WINDOW_SIZE), func(ts int64, kt KeyT, vt ValueT) error {
		msg := commtypes.Message{
			Key:       kt,
			Value:     vt,
			Timestamp: ts,
		}
		msgs = append(msgs, msg)
		return nil
	})
	ref_msgs := []commtypes.Message{
		{
			Key:       uint32(0),
			Value:     "zero",
			Timestamp: startTime,
		},
	}
	checkSlice(ref_msgs, msgs, t)

	err = store.Put(uint32(0), "zero", startTime)
	if err != nil {
		t.Fatalf("fail to put err: %v", err)
	}

	err = store.Put(uint32(0), "zero+", startTime)
	if err != nil {
		t.Fatalf("fail to put err: %v", err)
	}

	err = store.Put(uint32(0), "zero++", startTime)
	if err != nil {
		t.Fatalf("fail to put err: %v", err)
	}

	msgs = make([]commtypes.Message, 0)
	store.Fetch(uint32(0), time.UnixMilli(startTime-WINDOW_SIZE), time.UnixMilli(startTime+WINDOW_SIZE), func(ts int64, kt KeyT, vt ValueT) error {
		msg := commtypes.Message{
			Key:       kt,
			Value:     vt,
			Timestamp: ts,
		}
		msgs = append(msgs, msg)
		return nil
	})
	ref_msgs = []commtypes.Message{
		{
			Key:       uint32(0),
			Value:     "zero",
			Timestamp: startTime,
		},
		{
			Key:       uint32(0),
			Value:     "zero",
			Timestamp: startTime,
		},
		{
			Key:       uint32(0),
			Value:     "zero+",
			Timestamp: startTime,
		},
		{
			Key:       uint32(0),
			Value:     "zero++",
			Timestamp: startTime,
		},
	}
	checkSlice(ref_msgs, msgs, t)

	msgs = make([]commtypes.Message, 0)
	store.Fetch(uint32(0), time.UnixMilli(startTime+1-WINDOW_SIZE), time.UnixMilli(startTime+1+WINDOW_SIZE), func(ts int64, kt KeyT, vt ValueT) error {
		msg := commtypes.Message{
			Key:       kt,
			Value:     vt,
			Timestamp: ts,
		}
		msgs = append(msgs, msg)
		return nil
	})
	checkSlice(ref_msgs, msgs, t)

	msgs = make([]commtypes.Message, 0)
	store.Fetch(uint32(0), time.UnixMilli(startTime+2-WINDOW_SIZE), time.UnixMilli(startTime+2+WINDOW_SIZE), func(ts int64, kt KeyT, vt ValueT) error {
		msg := commtypes.Message{
			Key:       kt,
			Value:     vt,
			Timestamp: ts,
		}
		msgs = append(msgs, msg)
		return nil
	})
	checkSlice(ref_msgs, msgs, t)

	msgs = make([]commtypes.Message, 0)
	store.Fetch(uint32(0), time.UnixMilli(startTime+3-WINDOW_SIZE), time.UnixMilli(startTime+3+WINDOW_SIZE), func(ts int64, kt KeyT, vt ValueT) error {
		msg := commtypes.Message{
			Key:       kt,
			Value:     vt,
			Timestamp: ts,
		}
		msgs = append(msgs, msg)
		return nil
	})
	checkSlice(ref_msgs, msgs, t)

	msgs = make([]commtypes.Message, 0)
	store.Fetch(uint32(0), time.UnixMilli(startTime+4-WINDOW_SIZE), time.UnixMilli(startTime+4+WINDOW_SIZE), func(ts int64, kt KeyT, vt ValueT) error {
		msg := commtypes.Message{
			Key:       kt,
			Value:     vt,
			Timestamp: ts,
		}
		msgs = append(msgs, msg)
		return nil
	})
	ref_msgs = make([]commtypes.Message, 0)
	checkSlice(ref_msgs, msgs, t)
}
