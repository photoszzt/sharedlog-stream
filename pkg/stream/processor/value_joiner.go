package processor

type ValueJoiner interface {
	Apply(value1 interface{}, value2 interface{}) interface{}
}

type ValueJoinerFunc func(value1 interface{}, value2 interface{}) interface{}

func (fn ValueJoinerFunc) Apply(value1 interface{}, value2 interface{}) interface{} {
	return fn(value1, value2)
}

type ValueJoinerWithKey interface {
	Apply(readOnlyKey interface{}, value1 interface{}, value2 interface{}) interface{}
}

type ValueJoinerWithKeyFunc func(readOnlyKey interface{}, value1 interface{}, value2 interface{}) interface{}

func (fn ValueJoinerWithKeyFunc) Apply(readOnlyKey interface{}, value1 interface{}, value2 interface{}) interface{} {
	return fn(readOnlyKey, value1, value2)
}
