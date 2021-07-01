package operator

type MapFunc func(interface{}) interface{}

type Map struct {
	MapF MapFunc
	in   chan interface{}
	out  chan interface{}
}

func NewMap(mapFunc MapFunc) *Map {
	_map := &Map{
		mapFunc,
		make(chan interface{}),
		make(chan interface{}),
	}
	return _map
}

func (m *Map) Out() <-chan interface{} {
	return m.out
}

func (m *Map) In() chan<- interface{} {
	return m.in
}

func (m *Map) doStream() {
	for elem := range m.in {
		trans := m.MapF(elem)
		m.out <- trans
	}
}
