package processor

type Aggregator interface {
	Apply(key interface{}, value interface{}, aggregate interface{}) interface{}
}

type AggregatorG[K, V, VA any] interface {
	Apply(key K, value V, aggregate VA) VA
}

type AggregatorFunc func(key interface{}, value interface{}, aggregate interface{}) interface{}

func (fn AggregatorFunc) Apply(key interface{}, value interface{}, aggregate interface{}) interface{} {
	return fn(key, value, aggregate)
}

type AggregatorFuncG[K, V, VA any] func(key K, value V, agg VA) VA

func (fn AggregatorFuncG[K, V, VA]) Apply(key K, value V, aggregate VA) VA {
	return fn(key, value, aggregate)
}

type Initializer interface {
	Apply() interface{}
}

type InitializerFunc func() interface{}

func (fn InitializerFunc) Apply() interface{} {
	return fn()
}

type InitializerG[VA any] interface {
	Apply() VA
}

type InitializerFuncG[VA any] func() VA

func (fn InitializerFuncG[VA]) Apply() VA {
	return fn()
}
