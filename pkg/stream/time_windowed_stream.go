package stream

import "sharedlog-stream/pkg/stream/processor"

type WindowedKey struct {
	Key    interface{}
	Window processor.Window
}

type TimeWindowedStream interface {
	Count(name string) Table
	Aggregate(name string, initializer processor.Initializer, aggregator processor.Aggregator) Table
	Reduce(name string, reducer processor.Reducer) Table
}

type TimeWindowedStreamImpl struct {
	tp      *processor.TopologyBuilder
	parents []processor.Node
}

func newTimeWindowedStream(tp *processor.TopologyBuilder, parents []processor.Node) TimeWindowedStream {
	return &TimeWindowedStreamImpl{
		tp:      tp,
		parents: parents,
	}
}

func (s *TimeWindowedStreamImpl) Count(name string) Table {
	panic("Not implemented")
}

func (s *TimeWindowedStreamImpl) Aggregate(name string, initializer processor.Initializer, aggregator processor.Aggregator) Table {
	panic("Not implemented")
}

func (s *TimeWindowedStreamImpl) Reduce(name string, reducer processor.Reducer) Table {
	panic("Not implemented")
}
