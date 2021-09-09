package stream

import "cs.utexas.edu/zhitingz/sharedlog-stream/pkg/stream/processor"

func (s *Stream) Filter(name string, pred processor.Predicate) *Stream {
	p := processor.NewFilterProcessor(pred)
	n := s.tp.AddProcessor(name, p, s.parents)
	return newStream(s.tp, []processor.Node{n})
}

func (s *Stream) FilterFunc(name string, pred processor.PredicateFunc) *Stream {
	return s.Filter(name, pred)
}
