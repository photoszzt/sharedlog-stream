package processor

type ValueJoiner interface {
	Apply(value1 interface{}, value2 interface{}) interface{}
}

type ValueJointerFunc func(value1 interface{}, value2 interface{}) interface{}

func (fn ValueJointerFunc) Apply(value1 interface{}, value2 interface{}) interface{} {
	return fn(value1, value2)
}
