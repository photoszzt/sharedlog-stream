package stream

import "cs.utexas.edu/zhitingz/sharedlog-stream/pkg/stream/processor"

type Stream struct {
	tp      *processor.TopologyBuilder
	parents []processor.Node
}

func newStream(tp *processor.TopologyBuilder, parents []processor.Node) *Stream {
	return &Stream{
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

func (sb *StreamBuilder) Source(name string, source processor.Source) *Stream {
	n := sb.tp.AddSource(name, source)
	return newStream(sb.tp, []processor.Node{n})
}

func (sb *StreamBuilder) Build() (*processor.Topology, []error) {
	return sb.tp.Build()
}
