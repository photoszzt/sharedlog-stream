package stream

import "cs.utexas.edu/zhitingz/sharedlog-stream/pkg/stream/processor"

// Merge merges one or more streams into this stream.
func (s *Stream) Merge(name string, streams ...*Stream) *Stream {
	parents := []processor.Node{}
	parents = append(parents, s.parents...)
	for _, stream := range streams {
		parents = append(parents, stream.parents...)
	}

	p := processor.NewMergeProcessor()

	n := s.tp.AddProcessor(name, p, parents)

	return newStream(s.tp, []processor.Node{n})
}
