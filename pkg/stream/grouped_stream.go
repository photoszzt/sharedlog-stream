package stream

import "cs.utexas.edu/zhitingz/sharedlog-stream/pkg/stream/processor"

type GroupedStream interface {
	Count(name string) Table
	Reduce(name string, reducer processor.Reducer) Table
	Aggregate(name string, initializer processor.Initializer, aggregator processor.Aggregator) Table
	WindowedBy(windows EnumerableWindowDefinition) TimeWindowedStream
}
