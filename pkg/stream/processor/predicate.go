package processor

import "sharedlog-stream/pkg/stream/processor/commtypes"

var _ = (Predicate)(PredicateFunc(nil))

type PredicateFunc func(*commtypes.Message) (bool, error)

type Predicate interface {
	Assert(*commtypes.Message) (bool, error)
}

func (fn PredicateFunc) Assert(msg *commtypes.Message) (bool, error) {
	return fn(msg)
}
