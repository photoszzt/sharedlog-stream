package stream

func (s *Stream) Filter(name string, pred Predicate) *Stream {
	p := NewFilterProcessor(pred)
	n := s.tp.AddProcessor(name, p, s.parents)
	return newStream(s.tp, []Node{n})
}

func (s *Stream) FilterFunc(name string, pred PredicateFunc) *Stream {
	return s.Filter(name, pred)
}
