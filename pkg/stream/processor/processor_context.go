package processor

type ProcessorContext interface {
	RegisterStateStore(store StateStore)
}
