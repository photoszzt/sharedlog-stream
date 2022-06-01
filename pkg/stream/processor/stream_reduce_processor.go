package processor

import (
	"context"
	"sharedlog-stream/pkg/store"
	"sharedlog-stream/pkg/stream/processor/commtypes"

	"github.com/rs/zerolog/log"
)

type StreamReduceProcessor struct {
	pipe    Pipe
	store   store.KeyValueStore
	pctx    store.StoreContext
	reducer Reducer
}

var _ = Processor(&StreamReduceProcessor{})

func NewStreamReduceProcessor(reducer Reducer) *StreamReduceProcessor {
	return &StreamReduceProcessor{
		reducer: reducer,
	}
}

func (p *StreamReduceProcessor) WithPipe(pipe Pipe) {
	p.pipe = pipe
}

func (p *StreamReduceProcessor) WithProcessorContext(pctx store.StoreContext) {
	p.pctx = pctx
}

func (p *StreamReduceProcessor) Process(ctx context.Context, msg commtypes.Message) error {
	newMsg, err := p.ProcessAndReturn(ctx, msg)
	if err != nil {
		return err
	}
	return p.pipe.Forward(ctx, newMsg[0])
}

func (p *StreamReduceProcessor) ProcessAndReturn(ctx context.Context, msg commtypes.Message) ([]commtypes.Message, error) {
	if msg.Key == nil || msg.Value == nil {
		log.Warn().Msgf("skipping record due to null key or value. key=%v, val=%v", msg.Key, msg.Value)
		return nil, nil
	}
	val, ok, err := p.store.Get(ctx, msg.Key)
	if err != nil {
		return nil, err
	}
	var newAgg interface{}
	var newTs int64
	if ok {
		oldAggTs := val.(*commtypes.ValueTimestamp)
		newAgg = p.reducer.Apply(oldAggTs.Value, msg.Value)
		if msg.Timestamp > oldAggTs.Timestamp {
			newTs = msg.Timestamp
		} else {
			newTs = oldAggTs.Timestamp
		}
	} else {
		newAgg = msg.Value
		newTs = msg.Timestamp
	}
	err = p.store.Put(ctx, msg.Key, &commtypes.ValueTimestamp{Value: newAgg, Timestamp: newTs})
	if err != nil {
		return nil, err
	}
	return []commtypes.Message{{Key: msg.Key, Value: newAgg, Timestamp: newTs}}, nil
}
