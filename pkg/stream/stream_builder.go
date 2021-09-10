package stream

import "cs.utexas.edu/zhitingz/sharedlog-stream/pkg/stream/processor"

type StreamImpl struct {
	tp      *processor.TopologyBuilder
	parents []processor.Node
}

type Stream interface {
	Branch(name string, preds ...processor.Predicate) []Stream
	Filter(name string, pred processor.Predicate) Stream
	FlatMap(name string, mapper processor.FlatMapper) Stream
	Map(name string, mapper processor.Mapper) Stream
	Merge(name string, streams ...*StreamImpl) Stream
	Print(name string) Stream
	Process(name string, p processor.Processor) Stream
}

func newStream(tp *processor.TopologyBuilder, parents []processor.Node) Stream {
	return &StreamImpl{
		tp:      tp,
		parents: parents,
	}
}

type StreamBuilder struct {
	tp *processor.TopologyBuilder
}

func NewStreamBuilder() *StreamBuilder {
	return &StreamBuilder{
		tp: processor.NewTopologyBuilder(),
	}
}

func (sb *StreamBuilder) Source(name string, source processor.Source) Stream {
	n := sb.tp.AddSource(name, source)
	return newStream(sb.tp, []processor.Node{n})
}

func (s *StreamImpl) Branch(name string, preds ...processor.Predicate) []Stream {
	p := processor.NewBranchProcessor(preds)
	n := s.tp.AddProcessor(name, p, s.parents)

	streams := make([]Stream, 0, len(preds))
	for range preds {
		streams = append(streams, newStream(s.tp, []processor.Node{n}))
	}
	return streams
}

func (s *StreamImpl) Filter(name string, pred processor.Predicate) Stream {
	p := processor.NewFilterProcessor(pred)
	n := s.tp.AddProcessor(name, p, s.parents)
	return newStream(s.tp, []processor.Node{n})
}

// FlatMap runs a flat mapper on the stream.
func (s *StreamImpl) FlatMap(name string, mapper processor.FlatMapper) Stream {
	p := processor.NewFlatMapProcessor(mapper)
	n := s.tp.AddProcessor(name, p, s.parents)

	return newStream(s.tp, []processor.Node{n})
}

func (s *StreamImpl) Map(name string, mapper processor.Mapper) Stream {
	p := processor.NewMapProcessor(mapper)
	n := s.tp.AddProcessor(name, p, s.parents)
	return newStream(s.tp, []processor.Node{n})
}

// Merge merges one or more streams into this stream.
func (s *StreamImpl) Merge(name string, streams ...*StreamImpl) Stream {
	parents := []processor.Node{}
	parents = append(parents, s.parents...)
	for _, stream := range streams {
		parents = append(parents, stream.parents...)
	}

	p := processor.NewMergeProcessor()

	n := s.tp.AddProcessor(name, p, parents)

	return newStream(s.tp, []processor.Node{n})
}

// Print prints the data in the stream.
func (s *StreamImpl) Print(name string) Stream {
	return s.Process(name, processor.NewPrintProcessor())
}

// Process runs a custom processor on the stream.
func (s *StreamImpl) Process(name string, p processor.Processor) Stream {
	n := s.tp.AddProcessor(name, p, s.parents)

	return newStream(s.tp, []processor.Node{n})
}

func (sb *StreamBuilder) Build() (*processor.Topology, []error) {
	return sb.tp.Build()
}
