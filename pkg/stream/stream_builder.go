package stream

import (
	"sharedlog-stream/pkg/processor"
	"sharedlog-stream/pkg/producer_consumer"
)

type StreamBuilder struct {
	tp *processor.TopologyBuilder
}

func NewStreamBuilder() *StreamBuilder {
	return &StreamBuilder{
		tp: processor.NewTopologyBuilder(),
	}
}

func (sb *StreamBuilder) Source(name string, source producer_consumer.Consumer) Stream {
	n := sb.tp.AddSource(name, source)
	return newStream(sb.tp, []processor.Node{n})
}

func (sb *StreamBuilder) Build() (*processor.Topology, []error) {
	return sb.tp.Build()
}
