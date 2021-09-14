package state

// template type KeyValueIterator(Key, Value)

type Iterator interface {
	HasNext() bool
	Next() interface{}
	Close()
	PeekNextKey() interface{}
}

type KeyValueIterator interface {
	HasNext() bool
	Next() KeyValue
	Close()
	PeekNextKey() KeyT
}

type KeyValue struct {
	Key   KeyT
	Value ValueT
}
