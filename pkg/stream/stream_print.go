package stream

// Print prints the data in the stream.
func (s *Stream) Print(name string) *Stream {
	return s.Process(name, NewPrintProcessor())
}
