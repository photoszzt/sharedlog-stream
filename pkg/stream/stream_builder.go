package stream

type Stream struct {
	tp      *TopologyBuilder
	parents []Node
}

func newStream(tp *TopologyBuilder, parents []Node) *Stream {
	return &Stream{
		tp:      tp,
		parents: parents,
	}
}

type StreamBuilder struct {
	tp *TopologyBuilder
}

func NewStreamBuilder() *StreamBuilder {
	return &StreamBuilder{
		tp: NewTopologyBuilder(),
	}
}

func (sb *StreamBuilder) Source(name string, source Source) *Stream {
	n := sb.tp.AddSource(name, source)
	return newStream(sb.tp, []Node{n})
}

func (sb *StreamBuilder) Build() (*Topology, []error) {
	return sb.tp.Build()
}
