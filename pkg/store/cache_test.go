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
	cache := NewCache(func(entries []LRUElement[string, string]) {})
	for _, msg := range toInsert {
		cache.put(msg.Key.(string), LRUEntry[string]{value: optional.Of(msg.Value.(string)), isDirty: true})
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
	cache := NewCache(func(entries []LRUElement[int, int]) {})
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
	cache := NewCache(func(entries []LRUElement[int, int]) {})
	err := cache.put(0, LRUEntry[int]{value: optional.Of(10), isDirty: false})
	if err != nil {
		t.Errorf("expected nil, got %v", err)
	}
	cache.putIfAbsent(0, LRUEntry[int]{value: optional.Of(20), isDirty: false})
	cache.putIfAbsent(1, LRUEntry[int]{value: optional.Of(30), isDirty: false})
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
	cache := NewCache(func(entries []LRUElement[int, int]) {})
	cache.put(0, LRUEntry[int]{value: optional.Of(10), isDirty: false})
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
	cache := NewCache(func(entries []LRUElement[int, int]) {})
	cache.put(0, LRUEntry[int]{value: optional.Of(0), isDirty: false})
	cache.put(0, LRUEntry[int]{value: optional.Of(1), isDirty: false})
	cache.put(0, LRUEntry[int]{value: optional.Of(2), isDirty: false})
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
	cache := NewCache(func(entries []LRUElement[int, int]) {})
	cache.put(0, LRUEntry[int]{value: optional.Of(10), isDirty: false})
	cache.put(1, LRUEntry[int]{value: optional.Of(20), isDirty: false})
	cache.put(2, LRUEntry[int]{value: optional.Of(30), isDirty: false})
	cache.evict()
	_, found := cache.get(0)
	if found {
		t.Errorf("expected not found, got found")
	}
}

func TestShouldFlushDirtEntriesOnEviction(t *testing.T) {
	flushed := make([]LRUElement[int, int], 0)
	cache := NewCache(func(entries []LRUElement[int, int]) {
		flushed = append(flushed, entries...)
	})
	cache.put(0, LRUEntry[int]{value: optional.Of(10), isDirty: true})
	cache.put(1, LRUEntry[int]{value: optional.Of(20), isDirty: false})
	cache.put(2, LRUEntry[int]{value: optional.Of(30), isDirty: true})
	cache.evict()
	if len(flushed) != 2 {
		t.Errorf("expected 2, got %d", len(flushed))
	}
	if flushed[0].key != 0 {
		t.Errorf("expected 0, got %d", flushed[0].key)
	}
}
