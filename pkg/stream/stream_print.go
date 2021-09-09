package stream

import "cs.utexas.edu/zhitingz/sharedlog-stream/pkg/stream/processor"

// Print prints the data in the stream.
func (s *Stream) Print(name string) *Stream {
	return s.Process(name, processor.NewPrintProcessor())
}
