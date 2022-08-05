package processor

import (
	"context"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/store"
	"sharedlog-stream/pkg/utils"

	"github.com/rs/zerolog/log"
)

type StreamAggregateProcessor struct {
	store       store.CoreKeyValueStore
	initializer Initializer
	aggregator  Aggregator
	name        string
}

var _ = Processor(&StreamAggregateProcessor{})

func NewStreamAggregateProcessor(name string, store store.CoreKeyValueStore, initializer Initializer, aggregator Aggregator) *StreamAggregateProcessor {
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
	// debug.Fprintf(os.Stderr, "StreamAgg key %v, oldVal %v, newVal %v\n", msg.Key, oldAgg, newAgg)
	return []commtypes.Message{{Key: msg.Key, Value: change, Timestamp: newTs}}, nil
}

type StreamAggregateProcessorG[K, V, VA any] struct {
	store       store.CoreKeyValueStoreG[K, *commtypes.ValueTimestamp]
	initializer InitializerG[VA]
	aggregator  AggregatorG[K, V, VA]
	name        string
}

var _ = Processor(&StreamAggregateProcessorG[int, int, int]{})

func NewStreamAggregateProcessorG[K, V, VA any](name string, store store.CoreKeyValueStoreG[K, *commtypes.ValueTimestamp],
	initializer InitializerG[VA], aggregator AggregatorG[K, V, VA],
) *StreamAggregateProcessorG[K, V, VA] {
	return &StreamAggregateProcessorG[K, V, VA]{
		initializer: initializer,
		aggregator:  aggregator,
		store:       store,
		name:        name,
	}
}

func (p *StreamAggregateProcessorG[K, V, VA]) Name() string {
	return p.name
}

func (p *StreamAggregateProcessorG[K, V, VA]) ProcessAndReturn(ctx context.Context, msg commtypes.Message) ([]commtypes.Message, error) {
	if utils.IsNil(msg.Key) || utils.IsNil(msg.Value) {
		log.Warn().Msgf("skipping record due to null key or value. key=%v, val=%v", msg.Key, msg.Value)
		return nil, nil
	}
	key := msg.Key.(K)
	oldAggTs, ok, err := p.store.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	var oldAgg interface{}
	var newTs int64
	if ok {
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
	newAgg := p.aggregator.Apply(key, msg.Value.(V), oldAgg.(VA))
	err = p.store.Put(ctx, msg.Key.(K), commtypes.CreateValueTimestampOptional(newAgg, newTs))
	if err != nil {
		return nil, err
	}
	change := commtypes.Change{
		NewVal: newAgg,
		OldVal: oldAgg,
	}
	// debug.Fprintf(os.Stderr, "StreamAgg key %v, oldVal %v, newVal %v\n", msg.Key, oldAgg, newAgg)
	return []commtypes.Message{{Key: msg.Key, Value: change, Timestamp: newTs}}, nil
}
