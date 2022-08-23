package processor

import (
	"context"
	"fmt"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/store"
	"sharedlog-stream/pkg/utils"

	"4d63.com/optional"
)

type TableAggregateProcessor struct {
	store       store.CoreKeyValueStore
	initializer Initializer
	add         Aggregator
	remove      Aggregator
	name        string
}

var _ = Processor(&TableAggregateProcessor{})

func NewTableAggregateProcessor(name string,
	store store.CoreKeyValueStore,
	initializer Initializer,
	add Aggregator,
	remove Aggregator,
) *TableAggregateProcessor {
	return &TableAggregateProcessor{
		initializer: initializer,
		add:         add,
		remove:      remove,
		name:        name,
		store:       store,
	}
}

func (p *TableAggregateProcessor) Name() string {
	return p.name
}

func (p *TableAggregateProcessor) ProcessAndReturn(ctx context.Context, msg commtypes.Message) ([]commtypes.Message, error) {
	if utils.IsNil(msg.Key) {
		return nil, fmt.Errorf("msg key for table aggregate should not be nil")
	}
	val, ok, err := p.store.Get(ctx, msg.Key)
	if err != nil {
		return nil, err
	}
	var oldAggTs *commtypes.ValueTimestamp = nil
	var oldAgg interface{} = nil
	if ok {
		oldAggTs = val.(*commtypes.ValueTimestamp)
		oldAgg = oldAggTs.Value
	}
	newTs := msg.Timestamp
	var intermediateAgg interface{}

	// first try to remove the old val
	msgVal := msg.Value.(commtypes.Change)
	if !utils.IsNil(msgVal.OldVal) && !utils.IsNil(oldAgg) {
		intermediateAgg = p.remove.Apply(msg.Key, msgVal.OldVal, oldAgg)
		newTs = utils.MaxInt64(msg.Timestamp, oldAggTs.Timestamp)
	} else {
		intermediateAgg = oldAgg
	}

	// then try to add the new val
	var newAgg interface{}
	if !utils.IsNil(msgVal.NewVal) {
		var initAgg interface{}
		if utils.IsNil(intermediateAgg) {
			initAgg = p.initializer.Apply()
		} else {
			initAgg = intermediateAgg
		}
		newAgg = p.add.Apply(msg.Key, msgVal.NewVal, initAgg)
		if !utils.IsNil(oldAggTs) {
			newTs = utils.MaxInt64(msg.Timestamp, oldAggTs.Timestamp)
		}
	} else {
		newAgg = intermediateAgg
	}

	err = p.store.Put(ctx, msg.Key, commtypes.CreateValueTimestamp(newAgg, newTs))
	if err != nil {
		return nil, err
	}
	change := commtypes.Change{
		NewVal: newAgg,
		OldVal: oldAgg,
	}
	return []commtypes.Message{{Key: msg.Key, Value: change, Timestamp: newTs}}, nil
}

type TableAggregateProcessorG[K, V, VA any] struct {
	store       store.CoreKeyValueStoreG[K, commtypes.ValueTimestampG[VA]]
	initializer InitializerG[VA]
	add         AggregatorG[K, V, VA]
	remove      AggregatorG[K, V, VA]
	name        string
}

var _ = Processor(&TableAggregateProcessor{})

func NewTableAggregateProcessorG[K, V, VA any](name string,
	store store.CoreKeyValueStoreG[K, commtypes.ValueTimestampG[VA]],
	initializer InitializerG[VA],
	add AggregatorG[K, V, VA],
	remove AggregatorG[K, V, VA],
) *TableAggregateProcessorG[K, V, VA] {
	return &TableAggregateProcessorG[K, V, VA]{
		initializer: initializer,
		add:         add,
		remove:      remove,
		name:        name,
		store:       store,
	}
}

func (p *TableAggregateProcessorG[K, V, VA]) Name() string {
	return p.name
}

func (p *TableAggregateProcessorG[K, V, VA]) ProcessAndReturn(ctx context.Context, msg commtypes.Message) ([]commtypes.Message, error) {
	if utils.IsNil(msg.Key) {
		return nil, fmt.Errorf("msg key for table aggregate should not be nil")
	}
	key := msg.Key.(K)
	oldAggTs, ok, err := p.store.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	oldAgg := optional.Empty[VA]()
	if ok {
		oldAgg = optional.Of(oldAggTs.Value)
	}
	newTs := msg.Timestamp
	var intermediateAgg optional.Optional[VA]

	// first try to remove the old val
	msgVal := msg.Value.(commtypes.Change)
	oldAggUnwrap, hasOldAgg := oldAgg.Get()
	if !utils.IsNil(msgVal.OldVal) && hasOldAgg {
		intermediateAgg = optional.Of(p.remove.Apply(key, msgVal.OldVal.(V), oldAggUnwrap))
		newTs = utils.MaxInt64(msg.Timestamp, oldAggTs.Timestamp)
	} else {
		intermediateAgg = oldAgg
	}

	// then try to add the new val
	var newAgg optional.Optional[VA]
	if !utils.IsNil(msgVal.NewVal) {
		var initAgg VA
		midAgg, hasMidAgg := intermediateAgg.Get()
		if hasMidAgg {
			initAgg = midAgg
		} else {
			initAgg = p.initializer.Apply()
		}
		newAgg = optional.Of(p.add.Apply(key, msgVal.NewVal.(V), initAgg))
		if !utils.IsNil(oldAggTs) {
			newTs = utils.MaxInt64(msg.Timestamp, oldAggTs.Timestamp)
		}
	} else {
		newAgg = intermediateAgg
	}

	err = p.store.Put(ctx, key, commtypes.CreateValueTimestampGOptional(newAgg, newTs))
	if err != nil {
		return nil, err
	}
	var newA interface{}
	var oldA interface{}
	newA, hasNewAgg := newAgg.Get()
	oldA, hasOldA := oldAgg.Get()
	if !hasNewAgg {
		newA = nil
	}
	if !hasOldA {
		oldA = nil
	}
	change := commtypes.Change{
		NewVal: newA,
		OldVal: oldA,
	}
	return []commtypes.Message{{Key: msg.Key, Value: change, Timestamp: newTs}}, nil
}
