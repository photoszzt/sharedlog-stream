package stream

import "cs.utexas.edu/zhitingz/sharedlog-stream/pkg/stream/processor"

// FlatMap runs a flat mapper on the stream.
func (s *Stream) FlatMap(name string, mapper processor.FlatMapper) *Stream {
	p := processor.NewFlatMapProcessor(mapper)
	n := s.tp.AddProcessor(name, p, s.parents)

	return newStream(s.tp, []processor.Node{n})
}

// FlatMapFunc runs a flat mapper on the stream.
func (s *Stream) FlatMapFunc(name string, mapper processor.FlatMapperFunc) *Stream {
	return s.FlatMap(name, mapper)
}
