package stream

import "cs.utexas.edu/zhitingz/sharedlog-stream/pkg/stream/processor"

func (s *Stream) Branch(name string, preds ...processor.Predicate) []*Stream {
	p := processor.NewBranchProcessor(preds)
	n := s.tp.AddProcessor(name, p, s.parents)

	streams := make([]*Stream, 0, len(preds))
	for range preds {
		streams = append(streams, newStream(s.tp, []processor.Node{n}))
	}
	return streams
}

func (s *Stream) BranchFunc(name string, preds ...processor.PredicateFunc) []*Stream {
	ps := make([]processor.Predicate, len(preds))
	for i, fn := range preds {
		ps[i] = fn
	}
	return s.Branch(name, ps...)
}
