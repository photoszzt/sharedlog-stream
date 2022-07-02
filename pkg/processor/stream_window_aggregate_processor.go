package processor

import (
	"context"
	"fmt"
	"os"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/debug"
	"sharedlog-stream/pkg/store"

	"github.com/rs/zerolog/log"
)

type StreamWindowAggregateProcessor struct {
	store              store.WindowStore
	initializer        Initializer
	aggregator         Aggregator
	windows            EnumerableWindowDefinition
	name               string
	observedStreamTime int64
}

var _ = Processor(&StreamWindowAggregateProcessor{})

func NewStreamWindowAggregateProcessor(name string, store store.WindowStore, initializer Initializer,
	aggregator Aggregator, windows EnumerableWindowDefinition,
) *StreamWindowAggregateProcessor {
	return &StreamWindowAggregateProcessor{
		initializer:        initializer,
		aggregator:         aggregator,
		observedStreamTime: 0,
		store:              store,
		windows:            windows,
	}
}

func (p *StreamWindowAggregateProcessor) Name() string {
	return p.name
}

func (p *StreamWindowAggregateProcessor) ProcessAndReturn(ctx context.Context, msg commtypes.Message) ([]commtypes.Message, error) {
	if msg.Key == nil {
		log.Warn().Msgf("skipping record due to null key. key=%v, val=%v", msg.Key, msg.Value)
		return nil, nil
	}
	debug.Fprintf(os.Stderr, "stream window agg msg k %v, v %v, ts %d\n", msg.Key, msg.Value, msg.Timestamp)
	ts := msg.Timestamp
	if p.observedStreamTime < ts {
		p.observedStreamTime = ts
	}
	closeTime := p.observedStreamTime - p.windows.GracePeriodMs()
	matchedWindows, keys, err := p.windows.WindowsFor(ts)
	if err != nil {
		return nil, fmt.Errorf("windows for err %v", err)
	}
	newMsgs := make([]commtypes.Message, 0)
	for _, windowStart := range keys {
		window := matchedWindows[windowStart]
		windowEnd := window.End()
		if windowEnd > closeTime {
			var oldAgg interface{}
			var newTs int64
			val, exists, err := p.store.Get(ctx, msg.Key, windowStart)
			if err != nil {
				return nil, fmt.Errorf("win agg get err %v", err)
			}
			if exists {
				oldAggTs, ok := val.(*commtypes.ValueTimestamp)
				if !ok {
					oldAggTsTmp := val.(commtypes.ValueTimestamp)
					oldAggTs = &oldAggTsTmp
				}
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
			err = p.store.Put(ctx, msg.Key, commtypes.CreateValueTimestamp(newAgg, newTs), windowStart)
			if err != nil {
				return nil, fmt.Errorf("win agg put err %v", err)
			}
			newMsgs = append(newMsgs, commtypes.Message{
				Key:       &commtypes.WindowedKey{Key: msg.Key, Window: window},
				Value:     commtypes.Change{NewVal: newAgg, OldVal: oldAgg},
				Timestamp: newTs,
			})
		} else {
			log.Warn().Interface("key", msg.Key).
				Interface("value", msg.Value).
				Int64("timestamp", msg.Timestamp).Msg("Skipping record for expired window. ")
		}
	}
	return newMsgs, nil
}
