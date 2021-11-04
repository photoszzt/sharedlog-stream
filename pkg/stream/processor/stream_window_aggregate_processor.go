package processor

import (
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"sharedlog-stream/pkg/stream/processor/store"

	"github.com/rs/zerolog/log"
)

type StreamWindowAggregateProcessor struct {
	pipe               Pipe
	store              store.WindowStore
	pctx               store.ProcessorContext
	initializer        Initializer
	aggregator         Aggregator
	windows            EnumerableWindowDefinition
	observedStreamTime uint64
}

var _ = Processor(&StreamWindowAggregateProcessor{})

func NewStreamWindowAggregateProcessor(store store.WindowStore, initializer Initializer, aggregator Aggregator, windows EnumerableWindowDefinition) *StreamWindowAggregateProcessor {
	return &StreamWindowAggregateProcessor{
		initializer:        initializer,
		aggregator:         aggregator,
		observedStreamTime: 0,
		store:              store,
		windows:            windows,
	}
}

func (p *StreamWindowAggregateProcessor) WithPipe(pipe Pipe) {
	p.pipe = pipe
}

func (p *StreamWindowAggregateProcessor) WithProcessorContext(pctx store.ProcessorContext) {
	p.pctx = pctx
}

func (p *StreamWindowAggregateProcessor) Process(msg commtypes.Message) error {
	newMsgs, err := p.ProcessAndReturn(msg)
	if err != nil {
		return err
	}
	for _, newMsg := range newMsgs {
		p.pipe.Forward(newMsg)
	}
	return nil
}

func (p *StreamWindowAggregateProcessor) ProcessAndReturn(msg commtypes.Message) ([]commtypes.Message, error) {
	if msg.Key == nil {
		log.Warn().Msgf("skipping record due to null key. key=%v, val=%v", msg.Key, msg.Value)
		return nil, nil
	}
	ts := msg.Timestamp
	if p.observedStreamTime < ts {
		p.observedStreamTime = ts
	}
	closeTime := p.observedStreamTime - p.windows.GracePeriodMs()
	matchedWindows, err := p.windows.WindowsFor(ts)
	if err != nil {
		return nil, err
	}
	newMsgs := make([]commtypes.Message, 0)
	for windowStart, window := range matchedWindows {
		windowEnd := window.End()
		if windowEnd > closeTime {
			var oldAgg interface{}
			var newAgg interface{}
			var newTs uint64
			val, exists, err := p.store.Get(msg.Key, windowStart)
			if err != nil {
				return nil, err
			}
			if exists {
				oldAggTs := val.(*commtypes.ValueTimestamp)
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
			err = p.store.Put(msg.Key, &commtypes.ValueTimestamp{Value: newAgg, Timestamp: newTs}, windowStart)
			if err != nil {
				return nil, err
			}
			newMsgs = append(newMsgs, commtypes.Message{Key: commtypes.WindowedKey{Key: msg.Key, Window: window}, Value: newAgg, Timestamp: newTs})
		} else {
			log.Warn().Interface("key", msg.Key).
				Interface("value", msg.Value).
				Uint64("timestamp", msg.Timestamp).Msg("Skipping record for expired window. ")
		}
	}
	return newMsgs, nil
}
