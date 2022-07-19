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

type StreamAggregateProcessor[KIn, VIn, VAgg any] struct {
	store       store.KeyValueStore
	initializer Initializer[VAgg]
	aggregator  Aggregator[KIn, VIn, VAgg]
	name        string
}

var _ = Processor[int, int, int, commtypes.Change[int]](&StreamAggregateProcessor[int, int, int]{})

func NewStreamAggregateProcessor[KIn, VIn, VAgg any](name string, store store.KeyValueStore,
	initializer Initializer[VAgg], aggregator Aggregator[KIn, VIn, VAgg]) *StreamAggregateProcessor[KIn, VIn, VAgg] {
	return &StreamAggregateProcessor[KIn, VIn, VAgg]{
		initializer: initializer,
		aggregator:  aggregator,
		store:       store,
		name:        name,
	}
}

func (p *StreamAggregateProcessor[KIn, VIn, VAgg]) Name() string {
	return p.name
}

func (p *StreamAggregateProcessor[KIn, VIn, VAgg]) ProcessAndReturn(ctx context.Context, msg commtypes.Message[KIn, VIn]) ([]commtypes.Message[KIn, commtypes.Change[VAgg]], error) {
	if utils.IsNil(msg.Key) || utils.IsNil(msg.Value) {
		log.Warn().Msgf("skipping record due to null key or value. key=%v, val=%v", msg.Key, msg.Value)
		return nil, nil
	}
	val, ok, err := p.store.Get(ctx, msg.Key)
	if err != nil {
		return nil, err
	}
	var oldAgg VAgg
	var newTs int64
	if ok {
		oldAggTs := val.(*commtypes.ValueTimestamp)
		oldAgg = oldAggTs.Value.(VAgg)
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
	change := commtypes.Change[VAgg]{
		NewVal: &newAgg,
		OldVal: &oldAgg,
	}
	debug.Fprintf(os.Stderr, "StreamAgg key %v, oldVal %v, newVal %v\n", msg.Key, oldAgg, newAgg)
	ret := make([]commtypes.Message[KIn, commtypes.Change[VAgg]], 1)
	ret[0] = commtypes.Message[KIn, commtypes.Change[VAgg]]{
		Key:       msg.Key,
		Value:     change,
		Timestamp: newTs,
	}
	return ret, nil
}
