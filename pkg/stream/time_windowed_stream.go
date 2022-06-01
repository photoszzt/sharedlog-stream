package stream

import (
	"sharedlog-stream/pkg/concurrent_skiplist"
	"sharedlog-stream/pkg/store_with_changelog"
	"sharedlog-stream/pkg/stream/processor"
)

type TimeWindowedStream interface {
	Count(name string, mp *store_with_changelog.MaterializeParam, comparable concurrent_skiplist.Comparable) Table
	Aggregate(name string, initializer processor.Initializer,
		aggregator processor.Aggregator,
		mp *store_with_changelog.MaterializeParam, comparable concurrent_skiplist.Comparable) Table
	Reduce(name string, reducer processor.Reducer) Table
}

type TimeWindowedStreamImpl struct {
	windowDefs processor.EnumerableWindowDefinition
	tp         *processor.TopologyBuilder
	parents    []processor.Node
}

func newTimeWindowedStream(tp *processor.TopologyBuilder,
	parents []processor.Node, windowDefs processor.EnumerableWindowDefinition,
) TimeWindowedStream {
	return &TimeWindowedStreamImpl{
		tp:         tp,
		parents:    parents,
		windowDefs: windowDefs,
	}
}

func (s *TimeWindowedStreamImpl) Count(name string, mp *store_with_changelog.MaterializeParam,
	comparable concurrent_skiplist.Comparable,
) Table {
	store, err := store_with_changelog.NewInMemoryWindowStoreWithChangelog(
		s.windowDefs.MaxSize()+s.windowDefs.GracePeriodMs(),
		s.windowDefs.MaxSize(), false, comparable, mp)
	if err != nil {
		panic(err)
	}
	p := processor.NewStreamWindowAggregateProcessor(store,
		processor.InitializerFunc(func() interface{} {
			return 0
		}),
		processor.AggregatorFunc(func(key, value, aggregate interface{}) interface{} {
			val := aggregate.(uint64)
			return val + 1
		}), s.windowDefs)
	n := s.tp.AddProcessor(name, p, s.parents)
	_ = s.tp.AddWindowStore(mp.StoreName())
	return newTable(s.tp, []processor.Node{n}, mp.StoreName())
}

func (s *TimeWindowedStreamImpl) Aggregate(name string,
	initializer processor.Initializer,
	aggregator processor.Aggregator,
	mp *store_with_changelog.MaterializeParam,
	comparable concurrent_skiplist.Comparable,
) Table {
	store, err := store_with_changelog.NewInMemoryWindowStoreWithChangelog(
		s.windowDefs.MaxSize()+s.windowDefs.GracePeriodMs(),
		s.windowDefs.MaxSize(), false, comparable, mp)
	if err != nil {
		panic(err)
	}
	p := processor.NewStreamWindowAggregateProcessor(store, initializer, aggregator, s.windowDefs)
	n := s.tp.AddProcessor(name, p, s.parents)
	_ = s.tp.AddWindowStore(mp.StoreName())
	return newTable(s.tp, []processor.Node{n}, mp.StoreName())
}

func (s *TimeWindowedStreamImpl) Reduce(name string, reducer processor.Reducer) Table {
	panic("Not implemented")
}
