package processor

type Aggregator interface {
	Apply(key interface{}, value interface{}, aggregate interface{}) interface{}
}

type AggregatorFunc func(key interface{}, value interface{}, aggregate interface{}) interface{}

func (fn AggregatorFunc) Apply(key interface{}, value interface{}, aggregate interface{}) interface{} {
	return fn(key, value, aggregate)
}

type Initializer interface {
	Apply() interface{}
}

type InitializerFunc func() interface{}

func (fn InitializerFunc) Apply() interface{} {
	return fn()
}
