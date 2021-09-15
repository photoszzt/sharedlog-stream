package stream

import "cs.utexas.edu/zhitingz/sharedlog-stream/pkg/stream/processor"

type GroupedStream interface {
	Count(name string) Table
	Reduce(name string, reducer processor.Reducer) Table
	Aggregate(name string, initializer processor.Initializer, aggregator processor.Aggregator) Table
	WindowedBy(windows processor.EnumerableWindowDefinition) TimeWindowedStream
}

type GroupedStreamImpl struct {
	tp      *processor.TopologyBuilder
	parents []processor.Node
	grouped *Grouped
}

func newGroupedStream(tp *processor.TopologyBuilder, parents []processor.Node, grouped *Grouped) GroupedStream {
	return &GroupedStreamImpl{
		tp:      tp,
		parents: parents,
		grouped: grouped,
	}
}

func (s *GroupedStreamImpl) Count(name string) Table {
	panic("not implemented")
}

func (s *GroupedStreamImpl) Reduce(name string, reducer processor.Reducer) Table {
	panic("not implemented")
}

func (s *GroupedStreamImpl) Aggregate(name string, initializer processor.Initializer, aggregator processor.Aggregator) Table {
	panic("not implemented")
}

func (s *GroupedStreamImpl) WindowedBy(windows processor.EnumerableWindowDefinition) TimeWindowedStream {
	panic("not implemented")
}
