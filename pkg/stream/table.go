package stream

import "sharedlog-stream/pkg/stream/processor"

type Table interface {
	Filter(name string, pred processor.Predicate) Table
	FilterNot(name string, pred processor.Predicate) Table
	MapValues(name string, mapper processor.ValueMapper) Table
	MapValuesWithKey(name string, mapper processor.Mapper) Table
	Join(name string, other Table, joiner processor.ValueJoiner) Table
	LeftJoin(name string, other Table, joiner processor.ValueJoiner) Table
	OuterJoin(name string, other Table, joiner processor.ValueJoiner) Table
	ToStream() Stream
	Process(name string, p processor.Processor) Table
	ProcessWithStateStores(name string, p processor.Processor, stateStoreName ...string) Table
}
