package stream

import "cs.utexas.edu/zhitingz/sharedlog-stream/pkg/stream/processor"

type Table interface {
	Filter(name string, pred processor.Predicate) Table
	FilterNot(name string, pred processor.Predicate) Table
	MapValues(name string, mapper processor.ValueMapper) Table
}
