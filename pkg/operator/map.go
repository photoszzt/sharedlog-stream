package operator

type MapFunc func(interface{}) interface{}

type Map struct {
	MapF MapFunc
}

func NewMap(mapFunc MapFunc) *Map {
	_map := &Map{
		mapFunc,
	}
	return _map
}
