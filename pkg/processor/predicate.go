package processor

var _ = (Predicate)(PredicateFunc(nil))

type PredicateFunc func(key interface{}, value interface{}) (bool, error)

type Predicate interface {
	Assert(key interface{}, value interface{}) (bool, error)
}

func (fn PredicateFunc) Assert(key interface{}, value interface{}) (bool, error) {
	return fn(key, value)
}

type PredicateG[K, V any] interface {
	Assert(key K, value V) (bool, error)
}

type PredicateFuncG[K, V any] func(key K, value V) (bool, error)

func (fn PredicateFuncG[K, V]) Assert(key K, value V) (bool, error) {
	return fn(key, value)
}
