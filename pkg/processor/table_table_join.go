package processor

import (
	"context"
	"fmt"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/store"

	"github.com/rs/zerolog/log"
)

type TableTableJoinProcessor struct {
	store             store.KeyValueStore
	joiner            ValueJoinerWithKey
	streamTimeTracker commtypes.StreamTimeTracker
	name              string
}

var _ = Processor(&StreamTableJoinProcessor{})

func NewTableTableJoinProcessor(name string, store store.KeyValueStore, joiner ValueJoinerWithKey) *TableTableJoinProcessor {
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
		rv := val2.(commtypes.ValueTimestamp)
		if rv.Value == nil {
			return nil, nil
		}
		p.streamTimeTracker.UpdateStreamTime(&msg)
		ts := msg.Timestamp
		if ts < rv.Timestamp {
			ts = rv.Timestamp
		}
		if msg.Value != nil {
			joined := p.joiner.Apply(msg.Key, msg.Value, val2)
			newMsg := commtypes.Message{Key: msg.Key, Value: joined, Timestamp: ts}
			return []commtypes.Message{newMsg}, nil
		} else {
			return []commtypes.Message{{Key: msg.Key, Value: nil, Timestamp: ts}}, nil
		}
	}
	return nil, nil
}
