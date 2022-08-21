package store

import (
	"fmt"
	"os"
	"sharedlog-stream/pkg/data_structure/genericlist"
	"sharedlog-stream/pkg/data_structure/linkedhashset"
	"sharedlog-stream/pkg/stats"
	"sync"
	"sync/atomic"
	"unsafe"

	"4d63.com/optional"
)

type LRUEntry[V any] struct {
	value   optional.Optional[V]
	isDirty bool
}

type LRUElement[K, V any] struct {
	key   K
	entry LRUEntry[V]
}

func (lruE *LRUEntry[V]) MarkClean() {
	lruE.isDirty = false
}

func (lruE *LRUEntry[V]) IsDirty() bool {
	return lruE.isDirty
}

func (lruE *LRUEntry[V]) Value() optional.Optional[V] {
	return lruE.value
}

type Cache[K comparable, V any] struct {
	mux           sync.Mutex
	flushCallback func(entries []LRUElement[K, V])
	cache         map[K]*genericlist.Element[LRUElement[K, V]] // protected by mux
	dirtyKeys     *linkedhashset.LinkedHashSet[K]              // protected by mux
	orderList     *genericlist.List[LRUElement[K, V]]          // protected by mux
	hitRatio      stats.StatsCollector[float64]
	elementSize   uint64

	currentSizeBytes uint64
	numReadHits      uint64
	numReadMisses    uint64
	numOverwrites    uint64
	numFlushes       uint64
}

type FlushCallbackFunc[K comparable, V any] func(entries []LRUElement[K, V])

func NewCache[K comparable, V any](flushCallback FlushCallbackFunc[K, V]) *Cache[K, V] {
	var k K
	var v V
	elementSizeApprox := uint64(unsafe.Sizeof(k) + unsafe.Sizeof(v))
	return &Cache[K, V]{
		cache:            make(map[K]*genericlist.Element[LRUElement[K, V]]),
		dirtyKeys:        linkedhashset.New[K](),
		orderList:        genericlist.New[LRUElement[K, V]](),
		flushCallback:    flushCallback,
		hitRatio:         stats.NewStatsCollector[float64]("cache_hit_ratio", stats.DEFAULT_COLLECT_DURATION),
		elementSize:      elementSizeApprox,
		currentSizeBytes: 0,
		numReadHits:      0,
		numReadMisses:    0,
		numOverwrites:    0,
		numFlushes:       0,
	}
}

func (c *Cache[K, V]) hits() uint64 {
	return atomic.LoadUint64(&c.numReadHits)
}

func (c *Cache[K, V]) misses() uint64 {
	return atomic.LoadUint64(&c.numReadMisses)
}

func (c *Cache[K, V]) overwrites() uint64 {
	return atomic.LoadUint64(&c.numOverwrites)
}

func (c *Cache[K, V]) flushes() uint64 {
	return atomic.LoadUint64(&c.numFlushes)
}

func (c *Cache[K, V]) first() LRUEntry[V] {
	c.mux.Lock()
	defer c.mux.Unlock()
	return c.orderList.Front().Value.entry
}

func (c *Cache[K, V]) last() LRUEntry[V] {
	c.mux.Lock()
	ret := c.orderList.Back().Value.entry
	c.mux.Unlock()
	return ret
}

func (c *Cache[K, V]) head() *genericlist.Element[LRUElement[K, V]] {
	c.mux.Lock()
	ret := c.orderList.Front()
	c.mux.Unlock()
	return ret
}

func (c *Cache[K, V]) tail() *genericlist.Element[LRUElement[K, V]] {
	c.mux.Lock()
	ret := c.orderList.Back()
	c.mux.Unlock()
	return ret
}

func (c *Cache[K, V]) len() int {
	return len(c.cache)
}

func (c *Cache[K, V]) getInternalLockHeld(key K) *genericlist.Element[LRUElement[K, V]] {
	element, ok := c.cache[key]
	if ok {
		atomic.AddUint64(&c.numReadHits, 1)
		hitRadio := float64(atomic.LoadUint64(&c.numReadHits)) / float64(atomic.LoadUint64(&c.numReadHits)+atomic.LoadUint64(&c.numReadMisses))
		c.hitRatio.AddSample(hitRadio)
		return element
	} else {
		atomic.AddUint64(&c.numReadMisses, 1)
		return nil
	}
}

func (c *Cache[K, V]) updateLRULockHeld(element *genericlist.Element[LRUElement[K, V]]) {
	c.orderList.MoveToFront(element)
}

func (c *Cache[K, V]) get(key K) (LRUEntry[V], bool /* found */) {
	c.mux.Lock()
	element := c.getInternalLockHeld(key)
	if element != nil {
		c.updateLRULockHeld(element)
		c.mux.Unlock()
		return element.Value.entry, true
	} else {
		c.mux.Unlock()
		return LRUEntry[V]{}, false
	}
}

func (c *Cache[K, V]) put(key K, value LRUEntry[V]) error {
	if !value.IsDirty() && c.dirtyKeys.Contains(key) {
		return fmt.Errorf("Attempting to put a clean entrty for key[%v] into cache when it already contains a dirty entry for the same key", key)
	}
	c.mux.Lock()
	element := c.cache[key]
	if element != nil {
		atomic.AddUint64(&c.numOverwrites, 1)
		element.Value.entry = value
		c.updateLRULockHeld(element)
	} else {
		element := c.orderList.PushFront(LRUElement[K, V]{key: key, entry: value})
		c.cache[key] = element
	}
	if value.isDirty {
		c.dirtyKeys.Remove(key)
		c.dirtyKeys.Add(key)
	}
	c.mux.Unlock()
	return nil
}

func (c *Cache[K, V]) evict() {
	c.mux.Lock()
	element := c.orderList.Back()
	if element != nil {
		c.orderList.Remove(element)
		delete(c.cache, element.Value.key)
		if element.Value.entry.isDirty {
			c.flushLockHeld(element)
		}
	}
	c.mux.Unlock()
}

func (c *Cache[K, V]) flush(element *genericlist.Element[LRUElement[K, V]]) {
	c.mux.Lock()
	c.flushLockHeld(nil)
	c.mux.Unlock()
}

func (c *Cache[K, V]) flushLockHeld(evicted *genericlist.Element[LRUElement[K, V]]) {
	atomic.AddUint64(&c.numFlushes, 1)
	if c.dirtyKeys.Len() == 0 {
		return
	}
	entries := make([]LRUElement[K, V], 0, c.dirtyKeys.Len())
	deleted := make([]K, 0, c.dirtyKeys.Len())
	if evicted != nil {
		entries = append(entries, evicted.Value)
		c.dirtyKeys.Remove(evicted.Value.key)
	}
	c.dirtyKeys.IterateCb(func(key K) bool {
		element := c.getInternalLockHeld(key)
		if element == nil {
			fmt.Fprintf(os.Stderr, "key=%v found in dirty key set, but entry is nil\n", key)
			return false
		}
		entries = append(entries, c.cache[key].Value)
		element.Value.entry.MarkClean()
		if !element.Value.entry.value.IsPresent() {
			deleted = append(deleted, key)
		}
		return true
	})
	c.dirtyKeys.Clear()
	c.flushCallback(entries)
	for _, k := range deleted {
		c.deleteLockHeld(k)
	}
}

func (c *Cache[K, V]) deleteLockHeld(key K) (LRUEntry[V], bool) {
	element, ok := c.cache[key]
	if ok {
		delete(c.cache, key)
	} else {
		return LRUEntry[V]{}, false
	}
	c.orderList.Remove(element)
	c.dirtyKeys.Remove(key)
	return element.Value.entry, true
}

func (c *Cache[K, V]) delete(key K) (LRUEntry[V], bool) {
	c.mux.Lock()
	entry, ok := c.deleteLockHeld(key)
	c.mux.Unlock()
	return entry, ok
}

func (c *Cache[K, V]) putIfAbsent(key K, value LRUEntry[V]) (LRUEntry[V], bool, error) {
	originalValue, found := c.get(key)
	if !found {
		err := c.put(key, value)
		if err != nil {
			return LRUEntry[V]{}, false, err
		}
	}
	return originalValue, found, nil
}
