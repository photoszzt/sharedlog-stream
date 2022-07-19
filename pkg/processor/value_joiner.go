package processor

type ValueJoiner interface {
	Apply(value1 interface{}, value2 interface{}) interface{}
}

type ValueJoinerFunc func(value1 interface{}, value2 interface{}) interface{}

func (fn ValueJoinerFunc) Apply(value1 interface{}, value2 interface{}) interface{} {
	return fn(value1, value2)
}

type ValueJoinerWithKey[K any, V1 any, V2 any, Vout any] interface {
	Apply(readOnlyKey K, value1 V1, value2 V2) Vout
}

type ValueJoinerWithKeyFunc[K, V1, V2, Vout any] func(readOnlyKey K, value1 V1, value2 V2) Vout

func (fn ValueJoinerWithKeyFunc[K, V1, V2, Vout]) Apply(readOnlyKey K, value1 V1, value2 V2) Vout {
	return fn(readOnlyKey, value1, value2)
}

func ReverseValueJoinerWithKey[K, V1, V2, Vout any](f ValueJoinerWithKeyFunc[K, V1, V2, Vout]) ValueJoinerWithKeyFunc[K, V2, V1, Vout] {
	return func(readOnlyKey K, value2 V2, value1 V1) Vout {
		return f(readOnlyKey, value1, value2)
	}
}

type ValueJoinerWithKeyTs interface {
	Apply(readOnlyKey interface{}, value1 interface{}, value2 interface{},
		leftTs int64, otherTs int64) interface{}
}

type ValueJoinerWithKeyTsFunc func(readOnlyKey interface{}, value1 interface{}, value2 interface{},
	leftTs int64, otherTs int64) interface{}

func (fn ValueJoinerWithKeyTsFunc) Apply(readOnlyKey interface{}, value1 interface{}, value2 interface{},
	leftTs int64, otherTs int64,
) interface{} {
	return fn(readOnlyKey, value1, value2, leftTs, otherTs)
}

func ReverseValueJoinerWithKeyTs(f ValueJoinerWithKeyTsFunc) ValueJoinerWithKeyTsFunc {
	return func(readOnlyKey, value1, value2 interface{}, leftTs, otherTs int64) interface{} {
		return f(readOnlyKey, value2, value1, otherTs, leftTs)
	}
}
