package stream

import "sharedlog-stream/pkg/stream/processor"

type WindowedKey struct {
	Key    interface{}
	Window processor.Window
}

type TimeWindowedStream interface {
	Count(name string, mp *processor.MaterializeParam) Table
	Aggregate(name string, initializer processor.Initializer, aggregator processor.Aggregator, mp *processor.MaterializeParam) Table
	Reduce(name string, reducer processor.Reducer) Table
}

type TimeWindowedStreamImpl struct {
	tp         *processor.TopologyBuilder
	parents    []processor.Node
	windowDefs processor.EnumerableWindowDefinition
}

func newTimeWindowedStream(tp *processor.TopologyBuilder, parents []processor.Node, windowDefs processor.EnumerableWindowDefinition) TimeWindowedStream {
	return &TimeWindowedStreamImpl{
		tp:         tp,
		parents:    parents,
		windowDefs: windowDefs,
	}
}

func (s *TimeWindowedStreamImpl) Count(name string, mp *processor.MaterializeParam) Table {
	store := processor.NewInMemoryWindowStoreWithChangelog(mp.StoreName,
		s.windowDefs.MaxSize()+s.windowDefs.GracePeriodMs(),
		s.windowDefs.MaxSize(), mp)
	p := processor.NewStreamWindowAggregateProcessor(store,
		processor.InitializerFunc(func() interface{} {
			return 0
		}),
		processor.AggregatorFunc(func(key, value, aggregate interface{}) interface{} {
			val := aggregate.(uint64)
			return val + 1
		}), s.windowDefs)
	n := s.tp.AddProcessor(name, p, s.parents)
	_ = s.tp.AddWindowStore(mp.StoreName)
	return newTable(s.tp, []processor.Node{n}, mp.StoreName)
}

func (s *TimeWindowedStreamImpl) Aggregate(name string, initializer processor.Initializer, aggregator processor.Aggregator, mp *processor.MaterializeParam) Table {
	store := processor.NewInMemoryWindowStoreWithChangelog(mp.StoreName,
		s.windowDefs.MaxSize()+s.windowDefs.GracePeriodMs(),
		s.windowDefs.MaxSize(), mp)
	p := processor.NewStreamWindowAggregateProcessor(store, initializer, aggregator, s.windowDefs)
	n := s.tp.AddProcessor(name, p, s.parents)
	_ = s.tp.AddWindowStore(mp.StoreName)
	return newTable(s.tp, []processor.Node{n}, mp.StoreName)
}

func (s *TimeWindowedStreamImpl) Reduce(name string, reducer processor.Reducer) Table {
	panic("Not implemented")
}
