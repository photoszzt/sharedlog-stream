package stream

import "sharedlog-stream/pkg/stream/processor"

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

func (sb *StreamBuilder) Build() (*processor.Topology, []error) {
	return sb.tp.Build()
}
