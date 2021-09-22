package stream

import (
	"fmt"
	"os"
	"sharedlog-stream/pkg/stream/processor"
)

type GroupedStream interface {
	Count(name string, mp *processor.MaterializeParam) Table
	Reduce(name string, reducer processor.Reducer) Table
	Aggregate(name string, mp *processor.MaterializeParam, initializer processor.Initializer, aggregator processor.Aggregator) Table
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

func (s *GroupedStreamImpl) Count(name string, mp *processor.MaterializeParam) Table {
	store := processor.NewInMemoryKeyValueStoreWithChangelog(mp.StoreName,
		mp.KeySerde, mp.ValueSerde, mp.MsgSerde, mp.Changelog)
	p := processor.NewStreamAggregateProcessor(store,
		processor.InitializerFunc(func() interface{} {
			return 0
		}),
		processor.AggregatorFunc(func(key interface{}, value interface{}, agg interface{}) interface{} {
			val := agg.(uint64)
			return val + 1
		}))
	n := s.tp.AddProcessor(name, p, s.parents)
	return newTable(s.tp, []processor.Node{n})
}

func (s *GroupedStreamImpl) Reduce(name string, reducer processor.Reducer) Table {
	p := processor.NewStreamReduceProcessor(reducer)
	n := s.tp.AddProcessor(name, p, s.parents)
	return newTable(s.tp, []processor.Node{n})
}

func (s *GroupedStreamImpl) Aggregate(name string, mp *processor.MaterializeParam, initializer processor.Initializer, aggregator processor.Aggregator) Table {
	store := processor.NewInMemoryKeyValueStoreWithChangelog(mp.StoreName,
		mp.KeySerde, mp.ValueSerde, mp.MsgSerde, mp.Changelog)
	p := processor.NewStreamAggregateProcessor(store, initializer, aggregator)
	n := s.tp.AddProcessor(name, p, s.parents)
	return newTable(s.tp, []processor.Node{n})
}

func (s *GroupedStreamImpl) WindowedBy(windows processor.EnumerableWindowDefinition) TimeWindowedStream {
	fmt.Fprintf(os.Stderr, "window by is not implemented")
	panic("not implemented")
}
