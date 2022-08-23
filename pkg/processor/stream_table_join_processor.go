package processor

import (
	"context"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/store"

	"github.com/rs/zerolog/log"
)

type StreamTableJoinProcessor struct {
	store    store.CoreKeyValueStore
	joiner   ValueJoinerWithKey
	name     string
	leftJoin bool
}

var _ = Processor(&StreamTableJoinProcessor{})

func NewStreamTableJoinProcessor(store store.CoreKeyValueStore, joiner ValueJoinerWithKey) *StreamTableJoinProcessor {
	return &StreamTableJoinProcessor{
		joiner:   joiner,
		store:    store,
		leftJoin: false,
	}
}

func (p *StreamTableJoinProcessor) Name() string {
	return p.name
}

func (p *StreamTableJoinProcessor) ProcessAndReturn(ctx context.Context, msg commtypes.Message) ([]commtypes.Message, error) {
	if msg.Key == nil || msg.Value == nil {
		log.Warn().Msgf("Skipping record due to null join key or value. key=%v, val=%v", msg.Key, msg.Value)
		return nil, nil
	}
	val2, ok, err := p.store.Get(ctx, msg.Key)
	if err != nil {
		return nil, err
	}
	if p.leftJoin || ok {
		valAgg := val2.(*commtypes.ValueTimestamp)
		joined := p.joiner.Apply(msg.Key, msg.Value, valAgg.Value)
		newMsg := commtypes.Message{Key: msg.Key, Value: joined, Timestamp: msg.Timestamp}
		return []commtypes.Message{newMsg}, nil
	}
	return nil, nil
}

type StreamTableJoinProcessorG[K, V1, V2, VR any] struct {
	store    store.CoreKeyValueStoreG[K, commtypes.ValueTimestampG[V2]]
	joiner   ValueJoinerWithKeyG[K, V1, V2, VR]
	name     string
	leftJoin bool
}

var _ = Processor(&StreamTableJoinProcessorG[int, string, string, string]{})

func NewStreamTableJoinProcessorG[K, V1, V2, VR any](store store.CoreKeyValueStoreG[K, commtypes.ValueTimestampG[V2]],
	joiner ValueJoinerWithKeyG[K, V1, V2, VR]) *StreamTableJoinProcessorG[K, V1, V2, VR] {
	return &StreamTableJoinProcessorG[K, V1, V2, VR]{
		joiner:   joiner,
		store:    store,
		leftJoin: false,
	}
}

func (p *StreamTableJoinProcessorG[K, V1, V2, VR]) Name() string {
	return p.name
}

func (p *StreamTableJoinProcessorG[K, V1, V2, VR]) ProcessAndReturn(ctx context.Context, msg commtypes.Message) ([]commtypes.Message, error) {
	if msg.Key == nil || msg.Value == nil {
		log.Warn().Msgf("Skipping record due to null join key or value. key=%v, val=%v", msg.Key, msg.Value)
		return nil, nil
	}
	key := msg.Key.(K)
	valAgg, ok, err := p.store.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	if p.leftJoin || ok {
		joined := p.joiner.Apply(key, msg.Value.(V1), valAgg.Value)
		newMsg := commtypes.Message{Key: msg.Key, Value: joined, Timestamp: msg.Timestamp}
		return []commtypes.Message{newMsg}, nil
	}
	return nil, nil
}
