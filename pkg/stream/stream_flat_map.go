package stream

// FlatMap runs a flat mapper on the stream.
func (s *Stream) FlatMap(name string, mapper FlatMapper) *Stream {
	p := NewFlatMapProcessor(mapper)
	n := s.tp.AddProcessor(name, p, s.parents)

	return newStream(s.tp, []Node{n})
}

// FlatMapFunc runs a flat mapper on the stream.
func (s *Stream) FlatMapFunc(name string, mapper FlatMapperFunc) *Stream {
	return s.FlatMap(name, mapper)
}
