package processor

type Reducer interface {
	Apply(value1 interface{}, value2 interface{}) interface{}
}

type ReducerFunc func(value1 interface{}, value2 interface{}) interface{}

func (fn ReducerFunc) Apply(value1 interface{}, value2 interface{}) interface{} {
	return fn(value1, value2)
}
