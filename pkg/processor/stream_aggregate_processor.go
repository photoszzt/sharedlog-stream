package processor

import (
	"context"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/optional"
	"sharedlog-stream/pkg/store"
	"sharedlog-stream/pkg/utils"

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
	BaseProcessorG[K, V, K, commtypes.ChangeG[VA]]
	useCache bool
}

var _ = ProcessorG[int, int, int, commtypes.ChangeG[int]](&StreamAggregateProcessorG[int, int, int]{})

func NewStreamAggregateProcessorG[K, V, VA any](
	name string, store store.CoreKeyValueStoreG[K, commtypes.ValueTimestampG[VA]],
	initializer InitializerG[VA], aggregator AggregatorG[K, V, VA],
	useCache bool,
) ProcessorG[K, V, K, commtypes.ChangeG[VA]] {
	p := &StreamAggregateProcessorG[K, V, VA]{
		initializer: initializer,
		aggregator:  aggregator,
		store:       store,
		name:        name,
		useCache:    useCache,
	}
	p.BaseProcessorG.ProcessingFuncG = p.ProcessAndReturn
	if useCache {
		store.SetFlushCallback(func(ctx context.Context, msg commtypes.MessageG[K, commtypes.ChangeG[commtypes.ValueTimestampG[VA]]]) error {
			change := msg.Value.Unwrap()
			oldVal := optional.Map(change.OldVal, func(oldVal commtypes.ValueTimestampG[VA]) VA {
				return oldVal.Value
			})
			newVal := optional.Map(change.NewVal, func(newVal commtypes.ValueTimestampG[VA]) VA {
				return newVal.Value
			})
			ts := int64(0)
			if change.NewVal.IsSome() {
				newValTs := change.NewVal.Unwrap()
				ts = newValTs.Timestamp
			} else {
				ts = msg.Timestamp
			}
			v := commtypes.ChangeG[VA]{
				NewVal: newVal,
				OldVal: oldVal,
			}
			msgForNext := commtypes.MessageG[K, commtypes.ChangeG[VA]]{
				Key:       msg.Key,
				Value:     optional.Some(v),
				Timestamp: ts,
			}
			for _, nextProcessor := range p.nextProcessors {
				err := nextProcessor.Process(ctx, msgForNext)
				if err != nil {
					return err
				}
			}
			return nil
		})
	}
	return p
}

func (p *StreamAggregateProcessorG[K, V, VA]) Name() string {
	return p.name
}

func (p *StreamAggregateProcessorG[K, V, VA]) ProcessAndReturn(ctx context.Context,
	msg commtypes.MessageG[K, V],
) ([]commtypes.MessageG[K, commtypes.ChangeG[VA]], error) {
	if msg.Key.IsNone() || msg.Value.IsNone() {
		log.Warn().Msgf("skipping record due to null key or value. key=%v, val=%v", msg.Key, msg.Value)
		return nil, nil
	}
	key := msg.Key.Unwrap()
	oldAggTs, ok, err := p.store.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	var oldAgg optional.Option[VA]
	var newTs int64
	if ok {
		oldAgg = optional.Some(oldAggTs.Value)
		if msg.Timestamp > oldAggTs.Timestamp {
			newTs = msg.Timestamp
		} else {
			newTs = oldAggTs.Timestamp
		}
	} else {
		oldAgg = p.initializer.Apply()
		newTs = msg.Timestamp
	}
	msgVal := msg.Value.Unwrap()
	newAgg := p.aggregator.Apply(key, msgVal, oldAgg)
	err = p.store.Put(ctx, key, commtypes.CreateValueTimestampGOptional(newAgg, newTs), newTs)
	if err != nil {
		return nil, err
	}
	change := commtypes.ChangeG[VA]{
		NewVal: newAgg,
		OldVal: oldAgg,
	}
	// debug.Fprintf(os.Stderr, "StreamAgg key %v, oldVal %v, newVal %v\n", msg.Key, oldAgg, newAgg)
	if p.useCache {
		return nil, nil
	} else {
		return []commtypes.MessageG[K, commtypes.ChangeG[VA]]{{
			Key: msg.Key, Value: optional.Some(change), Timestamp: newTs}}, nil
	}
}
