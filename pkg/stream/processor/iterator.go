package processor

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

var (
	EMPTY_KEY_VALUE_ITER = EmptyKeyValueIterator{}
)

type EmptyKeyValueIterator struct {
}

func (e EmptyKeyValueIterator) HasNext() bool {
	return false
}

func (e EmptyKeyValueIterator) Next() KeyValue {
	panic("out of bound")
}

func (e EmptyKeyValueIterator) PeekNextKey() KeyT {
	panic("out of bound")
}

func (e EmptyKeyValueIterator) Close() {}
