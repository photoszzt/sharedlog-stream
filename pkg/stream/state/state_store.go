package state

type StateStoreContext interface {
}

type StateStore interface {
	Init(ctx StateStoreContext, root StateStore)
	Name() string
}

type KeyValueStore interface {
	StateStore
	Get(key interface{}) interface{}
	ApproximateNumEntries() uint64
	Put(key interface{}, value interface{})
	Delete(key interface{})
}
