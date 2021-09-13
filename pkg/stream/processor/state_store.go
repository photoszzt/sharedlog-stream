package processor

type StateStoreContext interface {
}

type StateStore interface {
	Init(ctx StateStoreContext, root StateStore)
	Name() string
}
