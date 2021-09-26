package processor

import "github.com/rs/zerolog/log"

type StreamWindowAggregateProcessor struct {
	pipe               Pipe
	store              WindowStore
	pctx               ProcessorContext
	initializer        Initializer
	aggregator         Aggregator
	observedStreamTime uint64
	windows            EnumerableWindowDefinition
}

var _ = Processor(&StreamWindowAggregateProcessor{})

func NewStreamWindowAggregateProcessor(initializer Initializer, aggregator Aggregator, windows EnumerableWindowDefinition) *StreamWindowAggregateProcessor {
	return &StreamWindowAggregateProcessor{
		initializer:        initializer,
		aggregator:         aggregator,
		observedStreamTime: 0,
	}
}

func (p *StreamWindowAggregateProcessor) WithPipe(pipe Pipe) {
	p.pipe = pipe
}

func (p *StreamWindowAggregateProcessor) WithProcessorContext(pctx ProcessorContext) {
	p.pctx = pctx
}

func (p *StreamWindowAggregateProcessor) Process(msg Message) error {
	if msg.Key == nil {
		log.Warn().Msgf("skipping record due to null key. key=%v, val=%v", msg.Key, msg.Value)
		return nil
	}
	ts := msg.Timestamp
	if p.observedStreamTime < ts {
		p.observedStreamTime = ts
	}
	closeTime := p.observedStreamTime - p.windows.GracePeriodMs()
	matchedWindows, err := p.windows.WindowsFor(ts)
	if err != nil {
		return err
	}
	for windowStart, window := range matchedWindows {
		windowEnd := window.End()
		if windowEnd > closeTime {
			var oldAgg interface{}
			var newAgg interface{}
			var newTs uint64
			val, exists := p.store.Get(msg.Key, windowStart)
			if exists {
				oldAggTs := val.(*ValueTimestamp)
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
			newAgg = p.aggregator.Apply(msg.Key, msg.Value, oldAgg)
			p.store.Put(msg.Key, &ValueTimestamp{Value: newAgg, Timestamp: newTs}, windowStart)
			p.pipe.Forward(Message{Key: msg.Key, Value: newAgg, Timestamp: newTs})
		} else {
			log.Warn().Interface("key", msg.Key).
				Interface("value", msg.Value).
				Uint64("timestamp", msg.Timestamp).Msg("Skipping record for expired window. ")
		}
	}
	return nil
}
