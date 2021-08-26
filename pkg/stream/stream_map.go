package stream

func (s *Stream) Map(name string, mapper Mapper) *Stream {
	p := NewMapProcessor(mapper)
	n := s.tp.AddProcessor(name, p, s.parents)
	return newStream(s.tp, []Node{n})
}

func (s *Stream) MapFunc(name string, mapper MapperFunc) *Stream {
	return s.Map(name, mapper)
}
