package stream

func (s *Stream) Branch(name string, preds ...Predicate) []*Stream {
	p := NewBranchProcessor(preds)
	n := s.tp.AddProcessor(name, p, s.parents)

	streams := make([]*Stream, 0, len(preds))
	for range preds {
		streams = append(streams, newStream(s.tp, []Node{n}))
	}
	return streams
}

func (s *Stream) BranchFunc(name string, preds ...PredicateFunc) []*Stream {
	ps := make([]Predicate, len(preds))
	for i, fn := range preds {
		ps[i] = fn
	}
	return s.Branch(name, ps...)
}
