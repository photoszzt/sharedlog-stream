package processor

import (
	"context"
	"fmt"
	"os"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/debug"
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
	if msg.Key == nil {
		return nil, fmt.Errorf("msg key for table aggregate should not be nil")
	}
	val, ok, err := p.store.Get(ctx, msg.Key)
	if err != nil {
		return nil, err
	}
	var oldAggTs *commtypes.ValueTimestamp
	var oldAgg interface{}
	if ok {
		oldAggTsTmp := val.(commtypes.ValueTimestamp)
		oldAggTs = &oldAggTsTmp
		oldAgg = oldAggTs.Value
	}
	newTs := msg.Timestamp
	var intermediateAgg interface{}

	// first try to remove the old val
	msgVal := msg.Value.(commtypes.Change)
	debug.Fprintf(os.Stderr, "msg key %v, msg newVal %v, msg oldVal %v, ts %d\n",
		msg.Key, msgVal.NewVal, msgVal.OldVal, msg.Timestamp)
	if msgVal.OldVal != nil && oldAgg != nil {
		intermediateAgg = p.remove.Apply(msg.Key, msgVal.OldVal, oldAgg)
		newTs = utils.MaxInt64(msg.Timestamp, oldAggTs.Timestamp)
	} else {
		intermediateAgg = oldAgg
	}

	// then try to add the new val
	var newAgg interface{}
	if msgVal.NewVal != nil {
		var initAgg interface{}
		if intermediateAgg == nil {
			initAgg = p.initializer.Apply()
		} else {
			initAgg = intermediateAgg
		}
		newAgg = p.add.Apply(msg.Key, msgVal.NewVal, initAgg)
		if oldAggTs != nil {
			newTs = utils.MaxInt64(msg.Timestamp, oldAggTs.Timestamp)
		}
	} else {
		newAgg = intermediateAgg
	}
	debug.Fprintf(os.Stderr, "k %v, newAgg %v, oldAgg %v\n", msg.Key, newAgg, oldAgg)

	err = p.store.Put(ctx, msg.Key, commtypes.ValueTimestamp{Timestamp: newTs, Value: newAgg})
	if err != nil {
		return nil, err
	}
	change := commtypes.Change{
		NewVal: newAgg,
		OldVal: oldAgg,
	}
	return []commtypes.Message{{Key: msg.Key, Value: change, Timestamp: newTs}}, nil
}
