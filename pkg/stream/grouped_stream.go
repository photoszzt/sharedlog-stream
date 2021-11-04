package stream

import (
	"sharedlog-stream/pkg/stream/processor"
	"sharedlog-stream/pkg/stream/processor/store"
)

type GroupedStream interface {
	Count(name string, mp *store.MaterializeParam) Table
	Reduce(name string, reducer processor.Reducer) Table
	Aggregate(name string, mp *store.MaterializeParam, initializer processor.Initializer, aggregator processor.Aggregator) Table
	WindowedBy(windows processor.EnumerableWindowDefinition) TimeWindowedStream
}

type GroupedStreamImpl struct {
	tp      *processor.TopologyBuilder
	grouped *Grouped
	parents []processor.Node
}

func newGroupedStream(tp *processor.TopologyBuilder, parents []processor.Node, grouped *Grouped) GroupedStream {
	return &GroupedStreamImpl{
		tp:      tp,
		parents: parents,
		grouped: grouped,
	}
}

func (s *GroupedStreamImpl) Count(name string, mp *store.MaterializeParam) Table {
	store := store.NewInMemoryKeyValueStoreWithChangelog(mp)
	p := processor.NewStreamAggregateProcessor(store,
		processor.InitializerFunc(func() interface{} {
			return 0
		}),
		processor.AggregatorFunc(func(key interface{}, value interface{}, agg interface{}) interface{} {
			val := agg.(uint64)
			return val + 1
		}))
	n := s.tp.AddProcessor(name, p, s.parents)
	_ = s.tp.AddKeyValueStore(store.Name())
	return newTable(s.tp, []processor.Node{n}, mp.StoreName)
}

func (s *GroupedStreamImpl) Reduce(name string, reducer processor.Reducer) Table {
	panic("Not fully implemented")
	/*
		p := processor.NewStreamReduceProcessor(reducer)
		n := s.tp.AddProcessor(name, p, s.parents)
		// TODO: add the table name
		return newTable(s.tp, []processor.Node{n}, "")
	*/
}

func (s *GroupedStreamImpl) Aggregate(name string, mp *store.MaterializeParam, initializer processor.Initializer, aggregator processor.Aggregator) Table {
	store := store.NewInMemoryKeyValueStoreWithChangelog(mp)
	p := processor.NewStreamAggregateProcessor(store, initializer, aggregator)
	n := s.tp.AddProcessor(name, p, s.parents)
	_ = s.tp.AddKeyValueStore(store.Name())
	return newTable(s.tp, []processor.Node{n}, mp.StoreName)
}

func (s *GroupedStreamImpl) WindowedBy(windows processor.EnumerableWindowDefinition) TimeWindowedStream {
	return newTimeWindowedStream(s.tp, s.parents, windows)
}
