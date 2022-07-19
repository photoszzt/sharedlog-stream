package processor

type Aggregator[K, V, VA any] interface {
	Apply(key K, value V, aggregate VA) VA
}

type AggregatorFunc[K, V, VA any] func(key K, value V, aggregate VA) VA

func (fn AggregatorFunc[K, V, VA]) Apply(key K, value V, aggregate VA) VA {
	return fn(key, value, aggregate)
}

type Initializer[VA any] interface {
	Apply() VA
}

type InitializerFunc[VA any] func() VA

func (fn InitializerFunc[VA]) Apply() VA {
	return fn()
}
