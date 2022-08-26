package processor

import (
	"context"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/store"
	"sharedlog-stream/pkg/utils"

	"4d63.com/optional"
	"github.com/rs/zerolog/log"
)

type StreamAggregateProcessor struct {
	store       store.CoreKeyValueStore
	initializer Initializer
	aggregator  Aggregator
	name        string
	BaseProcessor
}

var _ = Processor(&StreamAggregateProcessor{})

func NewStreamAggregateProcessor(name string, store store.CoreKeyValueStore, initializer Initializer, aggregator Aggregator) *StreamAggregateProcessor {
	p := &StreamAggregateProcessor{
		initializer:   initializer,
		aggregator:    aggregator,
		store:         store,
		name:          name,
		BaseProcessor: BaseProcessor{},
	}
	p.BaseProcessor.ProcessingFunc = p.ProcessAndReturn
	return p
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
	store       store.CoreKeyValueStoreG[K, commtypes.ValueTimestampG[VA]]
	initializer InitializerG[VA]
	aggregator  AggregatorG[K, V, VA]
	name        string
	BaseProcessor
}

var _ = Processor(&StreamAggregateProcessorG[int, int, int]{})

func NewStreamAggregateProcessorG[K, V, VA any](name string, store store.CoreKeyValueStoreG[K, commtypes.ValueTimestampG[VA]],
	initializer InitializerG[VA], aggregator AggregatorG[K, V, VA],
) *StreamAggregateProcessorG[K, V, VA] {
	p := &StreamAggregateProcessorG[K, V, VA]{
		initializer:   initializer,
		aggregator:    aggregator,
		store:         store,
		name:          name,
		BaseProcessor: BaseProcessor{},
	}
	p.BaseProcessor.ProcessingFunc = p.ProcessAndReturn
	return p
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
	var oldAgg VA
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
	newAgg := p.aggregator.Apply(key, msg.Value.(V), oldAgg)
	err = p.store.Put(ctx, key, commtypes.CreateValueTimestampGOptional(optional.Of(newAgg), newTs))
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
