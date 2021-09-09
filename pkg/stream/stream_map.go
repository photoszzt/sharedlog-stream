package stream

import "cs.utexas.edu/zhitingz/sharedlog-stream/pkg/stream/processor"

func (s *Stream) Map(name string, mapper processor.Mapper) *Stream {
	p := processor.NewMapProcessor(mapper)
	n := s.tp.AddProcessor(name, p, s.parents)
	return newStream(s.tp, []processor.Node{n})
}

func (s *Stream) MapFunc(name string, mapper processor.MapperFunc) *Stream {
	return s.Map(name, mapper)
}
