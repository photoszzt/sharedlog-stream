package stream

// Merge merges one or more streams into this stream.
func (s *Stream) Merge(name string, streams ...*Stream) *Stream {
	parents := []Node{}
	parents = append(parents, s.parents...)
	for _, stream := range streams {
		parents = append(parents, stream.parents...)
	}

	p := NewMergeProcessor()

	n := s.tp.AddProcessor(name, p, parents)

	return newStream(s.tp, []Node{n})
}
