package processor

import "github.com/rs/zerolog/log"

type StreamAggregateProcessor struct {
	pipe        Pipe
	store       KeyValueStore
	pctx        ProcessorContext
	initializer Initializer
	aggregator  Aggregator
}

var _ = Processor(&StreamAggregateProcessor{})

func NewStreamAggregateProcessor(initializer Initializer, aggregator Aggregator) *StreamAggregateProcessor {
	return &StreamAggregateProcessor{
		initializer: initializer,
		aggregator:  aggregator,
	}
}

func (p *StreamAggregateProcessor) WithPipe(pipe Pipe) {
	p.pipe = pipe
}

func (p *StreamAggregateProcessor) WithProcessorContext(pctx ProcessorContext) {
	p.pctx = pctx
}

func (p *StreamAggregateProcessor) Process(msg Message) error {
	if msg.Key == nil || msg.Value == nil {
		log.Warn().Msgf("skipping record due to null key or value. key=%v, val=%v", msg.Key, msg.Value)
		return nil
	}
	val, ok := p.store.Get(msg.Key)
	var oldAggTs *ValueTimestamp
	var oldAgg interface{}
	var newTs uint64
	if ok {
		oldAggTs = val.(*ValueTimestamp)
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
	p.store.Put(msg.Key, &ValueTimestamp{Timestamp: newTs, Value: newAgg})
	return p.pipe.Forward(Message{Key: msg.Key, Value: newAgg, Timestamp: newTs})
}
