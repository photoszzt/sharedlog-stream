package processor

import (
	"context"
	"fmt"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/store"
	"sharedlog-stream/pkg/utils"
)

type TableAggregateProcessor struct {
	store       store.KeyValueStore
	initializer Initializer
	add         Aggregator
	remove      Aggregator
	name        string
}

var _ = Processor(&TableAggregateProcessor{})

func NewTableAggregateProcessor(name string,
	store store.KeyValueStore,
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
