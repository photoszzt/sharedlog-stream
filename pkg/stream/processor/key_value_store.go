package processor

type KeyValueStore interface {
	StateStore
	Init(ctx ProcessorContext)
	Get(key KeyT) (ValueT, bool, error)
	Range(from KeyT, to KeyT) KeyValueIterator
	ReverseRange(from KeyT, to KeyT) KeyValueIterator
	PrefixScan(prefix interface{}, prefixKeyEncoder Encoder) KeyValueIterator
	ApproximateNumEntries() uint64
	Put(key KeyT, value ValueT) error
	PutIfAbsent(key KeyT, value ValueT) (ValueT, error)
	PutAll([]*Message) error
	Delete(key KeyT) error
}
