package stream

import (
	"github.com/rs/zerolog/log"

	"sharedlog-stream/pkg/stream/processor"
)

type StreamImpl struct {
	tp      *processor.TopologyBuilder
	parents []processor.Node
}

type Grouped struct {
	KeySerde   processor.Serde
	ValueSerde processor.Serde
	Name       string
}

func NewGrouped(keySerde processor.Serde, valueSerde processor.Serde, name string) *Grouped {
	return &Grouped{
		KeySerde:   keySerde,
		ValueSerde: valueSerde,
		Name:       name,
	}
}

type Stream interface {
	Branch(name string, preds ...processor.Predicate) []Stream
	Filter(name string, pred processor.Predicate) Stream
	FilterNot(name string, pred processor.Predicate) Stream
	FlatMap(name string, mapper processor.FlatMapper) Stream
	Map(name string, mapper processor.Mapper) Stream
	MapValues(name string, mapper processor.ValueMapper) Stream
	MapValuesWithKey(name string, mapper processor.Mapper) Stream
	Merge(name string, streams ...*StreamImpl) Stream
	Print(name string) Stream
	Process(name string, p processor.Processor) Stream
	ProcessWithStateStores(name string, p processor.Processor, stateStoreName ...string) Stream
	GroupBy(name string, mapper processor.Mapper, grouped *Grouped) GroupedStream
	GroupByKey(grouped *Grouped) GroupedStream
	StreamStreamJoin(other Stream, joiner processor.ValueJoinerWithKey, windows processor.JoinWindows) Stream
	StreamStreamLeftJoin(other Stream, joiner processor.ValueJoinerWithKey, windows processor.JoinWindows) Stream
	StreamStreamOuterJoin(other Stream, joiner processor.ValueJoinerWithKey, windows processor.JoinWindows) Stream
	StreamTableJoin(other Table, joiner processor.ValueJoinerWithKey) Stream
	StreamTableLeftJoin(other Table, joiner processor.ValueJoinerWithKey) Stream
	StreamTableOuterJoin(other Table, joiner processor.ValueJoinerWithKey) Stream
	ToTable(name string) Table
}

func newStream(tp *processor.TopologyBuilder, parents []processor.Node) Stream {
	return &StreamImpl{
		tp:      tp,
		parents: parents,
	}
}

func (s *StreamImpl) Branch(name string, preds ...processor.Predicate) []Stream {
	p := processor.NewBranchProcessor(preds)
	n := s.tp.AddProcessor(name, p, s.parents)

	streams := make([]Stream, 0, len(preds))
	for range preds {
		streams = append(streams, newStream(s.tp, []processor.Node{n}))
	}
	return streams
}

func (s *StreamImpl) Filter(name string, pred processor.Predicate) Stream {
	p := processor.NewStreamFilterProcessor(pred)
	n := s.tp.AddProcessor(name, p, s.parents)
	return newStream(s.tp, []processor.Node{n})
}

func (s *StreamImpl) FilterNot(name string, pred processor.Predicate) Stream {
	p := processor.NewStreamFilterNotProcessor(pred)
	n := s.tp.AddProcessor(name, p, s.parents)
	return newStream(s.tp, []processor.Node{n})
}

// FlatMap runs a flat mapper on the stream.
func (s *StreamImpl) FlatMap(name string, mapper processor.FlatMapper) Stream {
	p := processor.NewFlatMapProcessor(mapper)
	n := s.tp.AddProcessor(name, p, s.parents)

	return newStream(s.tp, []processor.Node{n})
}

func (s *StreamImpl) Map(name string, mapper processor.Mapper) Stream {
	p := processor.NewStreamMapProcessor(mapper)
	n := s.tp.AddProcessor(name, p, s.parents)
	return newStream(s.tp, []processor.Node{n})
}

func (s *StreamImpl) MapValues(name string, mapper processor.ValueMapper) Stream {
	p := processor.NewStreamMapValuesProcessor(mapper)
	n := s.tp.AddProcessor(name, p, s.parents)
	return newStream(s.tp, []processor.Node{n})
}

func (s *StreamImpl) MapValuesWithKey(name string, mapper processor.Mapper) Stream {
	p := processor.NewStreamMapValuesWithKeyProcessor(mapper)
	n := s.tp.AddProcessor(name, p, s.parents)
	return newStream(s.tp, []processor.Node{n})
}

// Merge merges one or more streams into this stream.
func (s *StreamImpl) Merge(name string, streams ...*StreamImpl) Stream {
	parents := []processor.Node{}
	parents = append(parents, s.parents...)
	for _, stream := range streams {
		parents = append(parents, stream.parents...)
	}

	p := processor.NewMergeProcessor()

	n := s.tp.AddProcessor(name, p, parents)

	return newStream(s.tp, []processor.Node{n})
}

// Print prints the data in the stream.
func (s *StreamImpl) Print(name string) Stream {
	return s.Process(name, processor.NewPrintProcessor())
}

// Process runs a custom processor on the stream.
func (s *StreamImpl) Process(name string, p processor.Processor) Stream {
	n := s.tp.AddProcessor(name, p, s.parents)

	return newStream(s.tp, []processor.Node{n})
}

func (s *StreamImpl) ProcessWithStateStores(name string, p processor.Processor, stateStoreNames ...string) Stream {
	log.Fatal().Msgf("Not implemented")
	return nil
}

func (s *StreamImpl) GroupBy(name string, mapper processor.Mapper, grouped *Grouped) GroupedStream {
	p := processor.NewStreamMapProcessor(mapper)
	n := s.tp.AddProcessor(grouped.Name, p, s.parents)
	n.(*processor.ProcessorNode).SetKeyChangingOp(true)
	return newGroupedStream(s.tp, []processor.Node{n}, grouped)
}

func (s *StreamImpl) GroupByKey(grouped *Grouped) GroupedStream {
	return newGroupedStream(s.tp, s.parents, grouped)
}

func (s *StreamImpl) StreamStreamJoin(other Stream, joiner processor.ValueJoinerWithKey, windows processor.JoinWindows) Stream {
	log.Fatal().Msgf("Not implemented")
	return nil
}

func (s *StreamImpl) StreamStreamLeftJoin(other Stream, joiner processor.ValueJoinerWithKey, windows processor.JoinWindows) Stream {
	log.Fatal().Msgf("Not implemented")
	return nil
}

func (s *StreamImpl) StreamStreamOuterJoin(other Stream, joiner processor.ValueJoinerWithKey, windows processor.JoinWindows) Stream {
	log.Fatal().Msgf("Not implemented")
	return nil
}

func (s *StreamImpl) StreamTableJoin(other Table, joiner processor.ValueJoinerWithKey) Stream {
	log.Fatal().Msgf("Not implemented")
	return nil
}

func (s *StreamImpl) StreamTableLeftJoin(other Table, joiner processor.ValueJoinerWithKey) Stream {
	log.Fatal().Msgf("Not implemented")
	return nil
}

func (s *StreamImpl) StreamTableOuterJoin(other Table, joiner processor.ValueJoinerWithKey) Stream {
	log.Fatal().Msgf("Not implemented")
	return nil
}

func (s *StreamImpl) ToTable(name string) Table {
	log.Fatal().Msgf("Not implemented")
	return nil
}
