package processor

import (
	"github.com/rs/zerolog/log"
)

type StreamAggregateProcessor struct {
	pipe        Pipe
	store       KeyValueStore
	pctx        ProcessorContext
	initializer Initializer
	aggregator  Aggregator
}

var _ = Processor(&StreamAggregateProcessor{})

func NewStreamAggregateProcessor(store KeyValueStore, initializer Initializer, aggregator Aggregator) *StreamAggregateProcessor {
	return &StreamAggregateProcessor{
		initializer: initializer,
		aggregator:  aggregator,
		store:       store,
	}
}

func (p *StreamAggregateProcessor) WithPipe(pipe Pipe) {
	p.pipe = pipe
}

func (p *StreamAggregateProcessor) WithProcessorContext(pctx ProcessorContext) {
	p.pctx = pctx
}

func (p *StreamAggregateProcessor) Process(msg Message) error {
	msg, err := p.ProcessAndReturn(msg)
	if err != nil {
		return err
	}
	return p.pipe.Forward(msg)
}

func (p *StreamAggregateProcessor) ProcessAndReturn(msg Message) (Message, error) {
	if msg.Key == nil || msg.Value == nil {
		log.Warn().Msgf("skipping record due to null key or value. key=%v, val=%v", msg.Key, msg.Value)
		return EmptyMessage, nil
	}
	val, ok, err := p.store.Get(msg.Key)
	if err != nil {
		return EmptyMessage, err
	}
	var oldAggTs ValueTimestamp
	var oldAgg interface{}
	var newTs uint64
	if ok {
		oldAggTs = val.(ValueTimestamp)
		oldAgg = oldAggTs.Value
		if msg.Timestamp > oldAggTs.Timestamp {
			newTs = msg.Timestamp
		} else {
			newTs = oldAggTs.Timestamp
		}
	} else {
		oldAgg = p.initializer.Apply()
		newTs = msg.Timestamp
	}
	newAgg := p.aggregator.Apply(msg.Key, msg.Value, oldAgg)
	err = p.store.Put(msg.Key, ValueTimestamp{Timestamp: newTs, Value: newAgg})
	if err != nil {
		return EmptyMessage, err
	}
	return Message{Key: msg.Key, Value: newAgg, Timestamp: newTs}, nil
}
