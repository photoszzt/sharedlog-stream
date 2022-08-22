package store

import (
	"sharedlog-stream/pkg/commtypes"
	"testing"

	"4d63.com/optional"
)

func TestShouldKeepTrackOfMostRecentlyAndLeastRecentlyUsed(t *testing.T) {
	toInsert := []commtypes.Message{
		{Key: "K1", Value: "V1"},
		{Key: "K2", Value: "V2"},
		{Key: "K3", Value: "V3"},
		{Key: "K4", Value: "V4"},
		{Key: "K5", Value: "V5"},
	}
	cache := NewCache(func(entries []LRUElement[string, string]) error { return nil },
		func(k string) int64 { return int64(len(k)) }, func(v string) int64 { return int64(len(v)) }, 4096)
	for _, msg := range toInsert {
		err := cache.put(msg.Key.(string), LRUEntry[string]{value: optional.Of(msg.Value.(string)), isDirty: true})
		if err != nil {
			t.Fatal(err)
		}
		head := cache.first()
		tail := cache.last()
		headV, _ := head.Value().Get()
		tailV, _ := tail.Value().Get()
		if headV != msg.Value.(string) {
			t.Errorf("expected %s, got %s", msg.Value.(string), headV)
		}
		if tailV != toInsert[0].Value.(string) {
			t.Errorf("expected %s, got %s", toInsert[0].Value.(string), tailV)
		}
		if cache.flushes() != 0 {
			t.Errorf("expected 0, got %d", cache.flushes())
		}
		if cache.hits() != 0 {
			t.Errorf("expected 0, got %d", cache.hits())
		}
		if cache.misses() != 0 {
			t.Errorf("expected 0, got %d", cache.misses())
		}
		if cache.overwrites() != 0 {
			t.Errorf("expected 0, got %d", cache.overwrites())
		}
	}
}

func TestShouldPutGet(t *testing.T) {
	cache := NewCache(func(entries []LRUElement[int, int]) error { return nil },
		commtypes.SizeOfInt, commtypes.SizeOfInt, 2048)
	err := cache.put(0, LRUEntry[int]{value: optional.Of(10), isDirty: false})
	if err != nil {
		t.Errorf("expected nil, got %v", err)
	}
	err = cache.put(1, LRUEntry[int]{value: optional.Of(11), isDirty: false})
	if err != nil {
		t.Errorf("expected nil, got %v", err)
	}
	err = cache.put(2, LRUEntry[int]{value: optional.Of(12), isDirty: false})
	if err != nil {
		t.Errorf("expected nil, got %v", err)
	}
	entry, found := cache.get(0)
	if !found {
		t.Errorf("expected found, got not found")
	}
	v, _ := entry.Value().Get()
	if v != 10 {
		t.Errorf("expected 10, got %d", v)
	}
	entry, found = cache.get(1)
	if !found {
		t.Errorf("expected found, got not found")
	}
	v, _ = entry.Value().Get()
	if v != 11 {
		t.Errorf("expected 10, got %d", v)
	}
	entry, found = cache.get(2)
	if !found {
		t.Errorf("expected found, got not found")
	}
	v, _ = entry.Value().Get()
	if v != 12 {
		t.Errorf("expected 10, got %d", v)
	}
}

func TestShouldPutIfAbsent(t *testing.T) {
	cache := NewCache(func(entries []LRUElement[int, int]) error { return nil },
		commtypes.SizeOfInt, commtypes.SizeOfInt, 2048)
	err := cache.put(0, LRUEntry[int]{value: optional.Of(10), isDirty: false})
	if err != nil {
		t.Errorf("expected nil, got %v", err)
	}
	_, _, err = cache.putIfAbsent(0, LRUEntry[int]{value: optional.Of(20), isDirty: false})
	if err != nil {
		t.Errorf("expected nil, got %v", err)
	}
	_, _, err = cache.putIfAbsent(1, LRUEntry[int]{value: optional.Of(30), isDirty: false})
	if err != nil {
		t.Errorf("expected nil, got %v", err)
	}
	entry, found := cache.get(0)
	if !found {
		t.Errorf("expected found, got not found")
	}
	v, _ := entry.Value().Get()
	if v != 10 {
		t.Errorf("expected 11, got %d", v)
	}
	entry, found = cache.get(1)
	if !found {
		t.Errorf("expected found, got not found")
	}
	v, _ = entry.Value().Get()
	if v != 30 {
		t.Errorf("expected 11, got %d", v)
	}
}

func TestShouldDeleteAndUpdateSize(t *testing.T) {
	cache := NewCache(func(entries []LRUElement[int, int]) error { return nil },
		commtypes.SizeOfInt, commtypes.SizeOfInt, 2048)
	err := cache.put(0, LRUEntry[int]{value: optional.Of(10), isDirty: false})
	if err != nil {
		t.Errorf("expected nil, got %v", err)
	}
	deleted, found := cache.delete(0)
	if !found {
		t.Errorf("expected found, got not found")
	}
	dVal, _ := deleted.Value().Get()
	if dVal != 10 {
		t.Errorf("expected 10, got %d", dVal)
	}
}

func TestShouldOverwriteAll(t *testing.T) {
	cache := NewCache(func(entries []LRUElement[int, int]) error { return nil },
		func(k int) int64 { return 4 }, func(v int) int64 { return 4 }, 2048)
	err := cache.put(0, LRUEntry[int]{value: optional.Of(0), isDirty: false})
	if err != nil {
		t.Errorf("expected nil, got %v", err)
	}
	err = cache.put(0, LRUEntry[int]{value: optional.Of(1), isDirty: false})
	if err != nil {
		t.Errorf("expected nil, got %v", err)
	}
	err = cache.put(0, LRUEntry[int]{value: optional.Of(2), isDirty: false})
	if err != nil {
		t.Errorf("expected nil, got %v", err)
	}
	entry, _ := cache.get(0)
	v, _ := entry.Value().Get()
	if v != 2 {
		t.Errorf("expected 2, got %d", v)
	}
	if cache.overwrites() != 2 {
		t.Errorf("expected 2, got %d", cache.overwrites())
	}
}

func TestShouldEvictEldestEntry(t *testing.T) {
	cache := NewCache(func(entries []LRUElement[int, int]) error { return nil },
		commtypes.SizeOfInt, commtypes.SizeOfInt, 2048)
	err := cache.put(0, LRUEntry[int]{value: optional.Of(10), isDirty: false})
	if err != nil {
		t.Errorf("expected nil, got %v", err)
	}
	err = cache.put(1, LRUEntry[int]{value: optional.Of(20), isDirty: false})
	if err != nil {
		t.Errorf("expected nil, got %v", err)
	}
	err = cache.put(2, LRUEntry[int]{value: optional.Of(30), isDirty: false})
	if err != nil {
		t.Errorf("expected nil, got %v", err)
	}
	cache.evict()
	_, found := cache.get(0)
	if found {
		t.Errorf("expected not found, got found")
	}
}

func TestShouldFlushDirtEntriesOnEviction(t *testing.T) {
	flushed := make([]LRUElement[int, int], 0)
	cache := NewCache(func(entries []LRUElement[int, int]) error {
		flushed = append(flushed, entries...)
		return nil
	}, func(k int) int64 { return 4 }, func(v int) int64 { return 4 }, 2048)
	err := cache.put(0, LRUEntry[int]{value: optional.Of(10), isDirty: true})
	if err != nil {
		t.Errorf("expected nil, got %v", err)
	}
	err = cache.put(1, LRUEntry[int]{value: optional.Of(20), isDirty: false})
	if err != nil {
		t.Errorf("expected nil, got %v", err)
	}
	err = cache.put(2, LRUEntry[int]{value: optional.Of(30), isDirty: true})
	if err != nil {
		t.Errorf("expected nil, got %v", err)
	}
	cache.evict()
	if len(flushed) != 2 {
		t.Errorf("expected 2, got %d, flushed contains: %+v", len(flushed), flushed)
	}
	if flushed[0].key != 0 {
		t.Errorf("expected 0, got %d", flushed[0].key)
	}
	v, _ := flushed[0].entry.value.Get()
	if v != 10 {
		t.Errorf("expected 10, got %d", v)
	}
	if flushed[1].key != 2 {
		t.Errorf("expected 2, got %d", flushed[1].key)
	}
	v, _ = flushed[1].entry.value.Get()
	if v != 30 {
		t.Errorf("expected 30, got %d", v)
	}
	if cache.flushes() != 1 {
		t.Errorf("expected 1, got %d", cache.flushes())
	}
}

func TestShouldRemoveDeletedValuesOnFlush(t *testing.T) {
	cache := NewCache(func(entries []LRUElement[int, int]) error { return nil },
		func(k int) int64 { return 4 }, func(v int) int64 { return 4 }, 2048)
	err := cache.put(0, LRUEntry[int]{value: optional.Empty[int](), isDirty: true})
	if err != nil {
		t.Errorf("expected nil, got %v", err)
	}
	err = cache.put(1, LRUEntry[int]{value: optional.Of(20), isDirty: true})
	if err != nil {
		t.Errorf("expected nil, got %v", err)
	}
	cache.flush(nil)
	if cache.len() != 1 {
		t.Errorf("expected 1, got %d", cache.len())
	}
	ret, ok := cache.get(1)
	if !ok {
		t.Errorf("expected found, got not found")
	}
	if !ret.value.IsPresent() {
		t.Errorf("expected present, got not present")
	}
}

func TestBasicPutGet(t *testing.T) {
	toInsert := []commtypes.MessageG[string, string]{
		{Key: "K1", Value: "V1"},
		{Key: "K2", Value: "V2"},
		{Key: "K3", Value: "V3"},
		{Key: "K4", Value: "V4"},
		{Key: "K5", Value: "V5"},
	}
	size := 5 * (4 + 3*ptrSize)
	cache := NewCache(func(entries []LRUElement[string, string]) error { return nil },
		commtypes.SizeOfString, commtypes.SizeOfString, size)
	for _, msg := range toInsert {
		err := cache.PutMaybeEvict(msg.Key, LRUEntry[string]{value: optional.Of(msg.Value), isDirty: true})
		if err != nil {
			t.Errorf("expected nil, got %v", err)
		}
	}

	for _, msg := range toInsert {
		entry, ok := cache.get(msg.Key)
		if !ok {
			t.Errorf("expected found, got not found")
		}
		v, _ := entry.Value().Get()
		if v != msg.Value {
			t.Errorf("expected %s, got %s", msg.Value, v)
		}
		if !entry.IsDirty() {
			t.Error("expected dirty, got clean")
		}
	}
}

func TestEvic(t *testing.T) {
	toInsert := []commtypes.MessageG[string, string]{
		{Key: "K1", Value: "V1"},
		{Key: "K2", Value: "V2"},
		{Key: "K3", Value: "V3"},
		{Key: "K4", Value: "V4"},
		{Key: "K5", Value: "V5"},
	}
	expected := []commtypes.MessageG[string, string]{{Key: "K1", Value: "V1"}}
	size := 4 + 3*ptrSize
	received := make([]commtypes.MessageG[string, string], 0, 5)
	cache := NewCache(func(entries []LRUElement[string, string]) error {
		for _, entry := range entries {
			v, ok := entry.entry.value.Get()
			if !ok {
				t.Errorf("expected present, got not present")
			}
			received = append(received, commtypes.MessageG[string, string]{Key: entry.key, Value: v})
		}
		return nil
	}, commtypes.SizeOfString, commtypes.SizeOfString, size)
	for _, msg := range toInsert {
		err := cache.PutMaybeEvict(msg.Key, LRUEntry[string]{value: optional.Of(msg.Value), isDirty: true})
		if err != nil {
			t.Errorf("expected nil, got %v", err)
		}
	}
	for idx, expect := range expected {
		if received[idx].Key != expect.Key {
			t.Errorf("expected %s, got %s", expect.Key, received[idx].Key)
		}
		if received[idx].Value != expect.Value {
			t.Errorf("expected %s, got %s", expect.Value, received[idx].Value)
		}
	}
	if cache.evicts() != 4 {
		t.Errorf("expected 4, got %d", cache.evicts())
	}
}
