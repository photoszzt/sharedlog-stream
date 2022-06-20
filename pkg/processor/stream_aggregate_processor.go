package processor

import (
	"context"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/store"

	"github.com/rs/zerolog/log"
)

type StreamAggregateProcessor struct {
	store       store.KeyValueStore
	initializer Initializer
	aggregator  Aggregator
	name        string
}

var _ = Processor(&StreamAggregateProcessor{})

func NewStreamAggregateProcessor(name string, store store.KeyValueStore, initializer Initializer, aggregator Aggregator) *StreamAggregateProcessor {
	return &StreamAggregateProcessor{
		initializer: initializer,
		aggregator:  aggregator,
		store:       store,
		name:        name,
	}
}

func (p *StreamAggregateProcessor) Name() string {
	return p.name
}

func (p *StreamAggregateProcessor) ProcessAndReturn(ctx context.Context, msg commtypes.Message) ([]commtypes.Message, error) {
	if msg.Key == nil || msg.Value == nil {
		log.Warn().Msgf("skipping record due to null key or value. key=%v, val=%v", msg.Key, msg.Value)
		return nil, nil
	}
	val, ok, err := p.store.Get(ctx, msg.Key)
	if err != nil {
		return nil, err
	}
	var oldAggTs commtypes.ValueTimestamp
	var oldAgg interface{}
	var newTs int64
	if ok {
		oldAggTs = val.(commtypes.ValueTimestamp)
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
	err = p.store.Put(ctx, msg.Key, commtypes.ValueTimestamp{Timestamp: newTs, Value: newAgg})
	if err != nil {
		return nil, err
	}
	return []commtypes.Message{{Key: msg.Key, Value: newAgg, Timestamp: newTs}}, nil
}
