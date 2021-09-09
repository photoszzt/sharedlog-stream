package stream

import "cs.utexas.edu/zhitingz/sharedlog-stream/pkg/stream/processor"

// Process runs a custom processor on the stream.
func (s *Stream) Process(name string, p processor.Processor) *Stream {
	n := s.tp.AddProcessor(name, p, s.parents)

	return newStream(s.tp, []processor.Node{n})
}
