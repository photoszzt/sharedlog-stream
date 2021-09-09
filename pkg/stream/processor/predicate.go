package processor

var _ = (Predicate)(PredicateFunc(nil))

type PredicateFunc func(Message) (bool, error)

type Predicate interface {
	Assert(Message) (bool, error)
}

func (fn PredicateFunc) Assert(msg Message) (bool, error) {
	return fn(msg)
}
