package processor

type KeyValueStore interface {
	StateStore
	Get(key KeyT) (ValueT, bool)
	Range(from KeyT, to KeyT) KeyValueIterator
	ReverseRange(from KeyT, to KeyT) KeyValueIterator
	PrefixScan(prefix interface{}, prefixKeyEncoder Encoder) KeyValueIterator
	ApproximateNumEntries() uint64
	Put(key KeyT, value ValueT)
	PutIfAbsent(key KeyT, value ValueT) ValueT
	PutAll([]*Message)
	Delete(key KeyT)
}
