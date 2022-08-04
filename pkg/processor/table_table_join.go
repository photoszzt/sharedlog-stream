package processor

import (
	"context"
	"fmt"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/store"

	"github.com/rs/zerolog/log"
)

type TableTableJoinProcessor struct {
	store             store.CoreKeyValueStore
	joiner            ValueJoinerWithKey
	streamTimeTracker commtypes.StreamTimeTracker
	name              string
}

var _ = Processor(&StreamTableJoinProcessor{})

func NewTableTableJoinProcessor(name string, store store.CoreKeyValueStore, joiner ValueJoinerWithKey) *TableTableJoinProcessor {
	return &TableTableJoinProcessor{
		joiner:            joiner,
		store:             store,
		streamTimeTracker: commtypes.NewStreamTimeTracker(),
		name:              name,
	}
}

func (p *TableTableJoinProcessor) Name() string {
	return p.name
}

func (p *TableTableJoinProcessor) ProcessAndReturn(ctx context.Context, msg commtypes.Message) ([]commtypes.Message, error) {
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
		var newVal interface{}
		var oldVal interface{}
		change := commtypes.CastToChangePtr(msg.Value)
		if change.NewVal != nil {
			newVal = p.joiner.Apply(msg.Key, change.NewVal, rvTs.Value)
		}
		if change.OldVal != nil {
			oldVal = p.joiner.Apply(msg.Key, change.OldVal, rvTs.Value)
		}
		return []commtypes.Message{{Key: msg.Key, Value: commtypes.Change{NewVal: newVal, OldVal: oldVal}, Timestamp: ts}}, nil
	}
	return nil, nil
}

type TableTableJoinProcessorG[K, V1, V2, VR any] struct {
	store             store.CoreKeyValueStoreG[K, *commtypes.ValueTimestamp]
	joiner            ValueJoinerWithKeyG[K, V1, V2, VR]
	streamTimeTracker commtypes.StreamTimeTracker
	name              string
}

var _ = Processor(&StreamTableJoinProcessorG[int, string, string, string]{})

func NewTableTableJoinProcessorG[K, V1, V2, VR any](name string, store store.CoreKeyValueStoreG[K, *commtypes.ValueTimestamp],
	joiner ValueJoinerWithKeyG[K, V1, V2, VR]) *TableTableJoinProcessorG[K, V1, V2, VR] {
	return &TableTableJoinProcessorG[K, V1, V2, VR]{
		joiner:            joiner,
		store:             store,
		streamTimeTracker: commtypes.NewStreamTimeTracker(),
		name:              name,
	}
}

func (p *TableTableJoinProcessorG[K, V1, V2, VR]) Name() string {
	return p.name
}

func (p *TableTableJoinProcessorG[K, V1, V2, VR]) ProcessAndReturn(ctx context.Context, msg commtypes.Message) ([]commtypes.Message, error) {
	if msg.Key == nil {
		log.Warn().Msgf("Skipping record due to null join key. key=%v", msg.Key)
		return nil, nil
	}
	key := msg.Key.(K)
	rvTs, ok, err := p.store.Get(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("get err: %v", err)
	}
	if ok {
		if rvTs.Value == nil {
			return nil, nil
		}
		ts := msg.Timestamp
		if ts < rvTs.Timestamp {
			ts = rvTs.Timestamp
		}
		var newVal interface{}
		var oldVal interface{}
		change := commtypes.CastToChangePtr(msg.Value)
		if change.NewVal != nil {
			newVal = p.joiner.Apply(key, change.NewVal.(V1), rvTs.Value.(V2))
		}
		if change.OldVal != nil {
			oldVal = p.joiner.Apply(key, change.OldVal.(V1), rvTs.Value.(V2))
		}
		return []commtypes.Message{{Key: msg.Key, Value: commtypes.Change{NewVal: newVal, OldVal: oldVal}, Timestamp: ts}}, nil
	}
	return nil, nil
}
