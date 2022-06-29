package processor

var _ = (Predicate)(PredicateFunc(nil))

type PredicateFunc func(key interface{}, value interface{}) (bool, error)

type Predicate interface {
	Assert(key interface{}, value interface{}) (bool, error)
}

func (fn PredicateFunc) Assert(key interface{}, value interface{}) (bool, error) {
	return fn(key, value)
}
