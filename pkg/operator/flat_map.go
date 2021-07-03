package operator

import "cs.utexas.edu/zhitingz/sharedlog-stream/pkg/stream"

type FlatMapFunc func(interface{}) []interface{}

type FlatMap struct {
	FlatMapF FlatMapFunc
	in       chan interface{}
	out      chan interface{}
}

var _ stream.Flow = (*FlatMap)(nil)

func NewFlatMap(flatMapFunc FlatMapFunc) *FlatMap {
	flatMap := &FlatMap{
		flatMapFunc,
		make(chan interface{}),
		make(chan interface{}),
	}
	go flatMap.doStream()
	return flatMap
}

func (fm *FlatMap) In() chan<- interface{} {
	return fm.in
}

func (fm *FlatMap) Out() <-chan interface{} {
	return fm.out
}

func (fm *FlatMap) doStream() {
	for elem := range fm.in {
		trans := fm.FlatMapF(elem)
		for _, item := range trans {
			fm.out <- item
		}
	}
}
