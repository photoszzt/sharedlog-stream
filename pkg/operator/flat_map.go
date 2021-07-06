package operator

type FlatMapFunc func(interface{}) []interface{}

type FlatMap struct {
	FlatMapF FlatMapFunc
}

func NewFlatMap(flatMapFunc FlatMapFunc) *FlatMap {
	flatMap := &FlatMap{
		flatMapFunc,
	}
	return flatMap
}
