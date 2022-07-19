package processor

import (
	"context"
	"fmt"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/store"

	"github.com/rs/zerolog/log"
)

type TableTableJoinProcessor[K, V1, V2, Vout any] struct {
	store             store.KeyValueStore
	joiner            ValueJoinerWithKey[K, V1, V2, Vout]
	streamTimeTracker commtypes.StreamTimeTracker
	name              string
}

var _ = Processor(&StreamTableJoinProcessor{})

func NewTableTableJoinProcessor[K, V1, V2, Vout any](name string, store store.KeyValueStore,
	joiner ValueJoinerWithKey[K, V1, V2, Vout],
) *TableTableJoinProcessor[K, V1, V2, Vout] {
	return &TableTableJoinProcessor[K, V1, V2, Vout]{
		joiner:            joiner,
		store:             store,
		streamTimeTracker: commtypes.NewStreamTimeTracker(),
		name:              name,
	}
}

func (p *TableTableJoinProcessor[K, V1, V2, Vout]) Name() string {
	return p.name
}

func (p *TableTableJoinProcessor[K, V1, V2, Vout]) ProcessAndReturn(ctx context.Context,
	msg commtypes.Message[K, commtypes.Change[V1]],
) ([]commtypes.Message[K, commtypes.Change[Vout]], error) {
	if msg.Key == nil {
		log.Warn().Msgf("Skipping record due to null join key. key=%v", msg.Key)
		return nil, nil
	}
	val2, ok, err := p.store.Get(ctx, msg.Key)
	if err != nil {
		return nil, fmt.Errorf("get err: %v", err)
	}
	if ok {
		rvTs := val2.(*commtypes.ValueTimestamp)
		if rvTs.Value == nil {
			return nil, nil
		}
		ts := msg.Timestamp
		if ts < rvTs.Timestamp {
			ts = rvTs.Timestamp
		}
		var newVal Vout
		var oldVal Vout
		change := msg.Value
		if change.NewVal != nil {
			newVal = p.joiner.Apply(msg.Key, change.NewVal, rvTs.Value)
		}
		if change.OldVal != nil {
			oldVal = p.joiner.Apply(msg.Key, change.OldVal, rvTs.Value)
		}
		return []commtypes.Message[K, commtypes.Change[Vout]]{
			{Key: msg.Key, Value: commtypes.Change[Vout]{NewVal: newVal, OldVal: oldVal}, Timestamp: ts},
		}, nil
	}
	return nil, nil
}
