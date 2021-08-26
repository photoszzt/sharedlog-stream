package stream

// Process runs a custom processor on the stream.
func (s *Stream) Process(name string, p Processor) *Stream {
	n := s.tp.AddProcessor(name, p, s.parents)

	return newStream(s.tp, []Node{n})
}
