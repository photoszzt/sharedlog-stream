package stream

import "sharedlog-stream/pkg/processor"

type Table interface {
	Filter(name string, pred processor.Predicate, queryableName string) Table
	FilterNot(name string, pred processor.Predicate, queryableName string) Table
	MapValues(name string, mapper processor.ValueMapper, queryableName string) Table
	MapValuesWithKey(name string, mapper processor.Mapper, queryableName string) Table
	Join(name string, other Table, joiner processor.ValueJoiner) Table
	LeftJoin(name string, other Table, joiner processor.ValueJoiner) Table
	OuterJoin(name string, other Table, joiner processor.ValueJoiner) Table
	ToStream() Stream
	Process(name string, p processor.Processor) Table
	ProcessWithStateStores(name string, p processor.Processor, stateStoreName ...string) Table
	StoreName() string
}

type TableImpl struct {
	tp        *processor.TopologyBuilder
	storeName string
	parents   []processor.Node
}

func newTable(tp *processor.TopologyBuilder, parents []processor.Node, storeName string) Table {
	return &TableImpl{
		tp:        tp,
		parents:   parents,
		storeName: storeName,
	}
}

func (t *TableImpl) StoreName() string {
	return t.storeName
}

func (t *TableImpl) Filter(name string, pred processor.Predicate, queryableName string) Table {
	p := processor.NewTableFilterProcessor(pred, false, queryableName)
	n := t.tp.AddProcessor(name, p, t.parents)
	return newTable(t.tp, []processor.Node{n}, t.storeName)
}

func (t *TableImpl) FilterNot(name string, pred processor.Predicate, queryableName string) Table {
	p := processor.NewTableFilterProcessor(pred, true, queryableName)
	n := t.tp.AddProcessor(name, p, t.parents)
	return newTable(t.tp, []processor.Node{n}, t.storeName)
}

func (t *TableImpl) MapValues(name string, mapper processor.ValueMapper, queryableName string) Table {
	p := processor.NewTableMapValuesProcessor(mapper, queryableName)
	n := t.tp.AddProcessor(name, p, t.parents)
	return newTable(t.tp, []processor.Node{n}, t.storeName)
}

func (t *TableImpl) MapValuesWithKey(name string, mapper processor.Mapper, queryableName string) Table {
	panic("not implemented")
}

func (t *TableImpl) Join(name string, other Table, joiner processor.ValueJoiner) Table {
	panic("not implemented")
}

func (t *TableImpl) LeftJoin(name string, other Table, joiner processor.ValueJoiner) Table {
	panic("not implemented")
}

func (t *TableImpl) OuterJoin(name string, other Table, joiner processor.ValueJoiner) Table {
	panic("not implemented")
}

func (t *TableImpl) ToStream() Stream {
	return newStream(t.tp, t.parents)
}

func (t *TableImpl) Process(name string, p processor.Processor) Table {
	panic("not implemented")
}

func (t *TableImpl) ProcessWithStateStores(name string, p processor.Processor, stateStoreName ...string) Table {
	panic("not implemented")
}
