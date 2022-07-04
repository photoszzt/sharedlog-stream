package processor

import (
	"context"
	"os"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/debug"
	"sharedlog-stream/pkg/store"
	"sharedlog-stream/pkg/utils"

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
	if utils.IsNil(msg.Key) || utils.IsNil(msg.Value) {
		log.Warn().Msgf("skipping record due to null key or value. key=%v, val=%v", msg.Key, msg.Value)
		return nil, nil
	}
	val, ok, err := p.store.Get(ctx, msg.Key)
	if err != nil {
		return nil, err
	}
	var oldAgg interface{}
	var newTs int64
	if ok {
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
	newAgg := p.aggregator.Apply(msg.Key, msg.Value, oldAgg)
	err = p.store.Put(ctx, msg.Key, commtypes.CreateValueTimestamp(newAgg, newTs))
	if err != nil {
		return nil, err
	}
	change := commtypes.Change{
		NewVal: newAgg,
		OldVal: oldAgg,
	}
	debug.Fprintf(os.Stderr, "StreamAgg key %v, oldVal %v, newVal %v\n", msg.Key, oldAgg, newAgg)
	return []commtypes.Message{{Key: msg.Key, Value: &change, Timestamp: newTs}}, nil
}
