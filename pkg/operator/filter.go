package operator

import "cs.utexas.edu/zhitingz/sharedlog-stream/pkg/stream"

type FilterFunc func(interface{}) bool

type Filter struct {
	FilterF FilterFunc
	in      chan interface{}
	out     chan interface{}
}

var _ stream.Flow = (*Filter)(nil)

func NewFilter(filterFunc FilterFunc) *Filter {
	_filter := &Filter{
		filterFunc,
		make(chan interface{}),
		make(chan interface{}),
	}
	go _filter.doStream()
	return _filter
}

func (f *Filter) Out() <-chan interface{} {
	return f.out
}

func (f *Filter) In() chan<- interface{} {
	return f.in
}

func (f *Filter) doStream() {
	for elem := range f.in {
		if f.FilterF(elem) {
			f.out <- elem
		}
	}
}
