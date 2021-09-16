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
