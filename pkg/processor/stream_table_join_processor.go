package processor

import (
	"context"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/store"

	"github.com/rs/zerolog/log"
)

type StreamTableJoinProcessor struct {
	store    store.KeyValueStore
	joiner   ValueJoinerWithKey
	name     string
	leftJoin bool
}

var _ = Processor(&StreamTableJoinProcessor{})

func NewStreamTableJoinProcessor(store store.KeyValueStore, joiner ValueJoinerWithKey) *StreamTableJoinProcessor {
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
		joined := p.joiner.Apply(msg.Key, msg.Value, val2)
		newMsg := commtypes.Message{Key: msg.Key, Value: joined, Timestamp: msg.Timestamp}
		return []commtypes.Message{newMsg}, nil
	}
	return nil, nil
}
