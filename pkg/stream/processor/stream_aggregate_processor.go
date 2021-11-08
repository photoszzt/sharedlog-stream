package processor

import (
	"context"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"sharedlog-stream/pkg/stream/processor/store"

	"github.com/rs/zerolog/log"
)

type StreamAggregateProcessor struct {
	pipe        Pipe
	store       store.KeyValueStore
	pctx        store.ProcessorContext
	initializer Initializer
	aggregator  Aggregator
}

var _ = Processor(&StreamAggregateProcessor{})

func NewStreamAggregateProcessor(store store.KeyValueStore, initializer Initializer, aggregator Aggregator) *StreamAggregateProcessor {
	return &StreamAggregateProcessor{
		initializer: initializer,
		aggregator:  aggregator,
		store:       store,
	}
}

func (p *StreamAggregateProcessor) WithPipe(pipe Pipe) {
	p.pipe = pipe
}

func (p *StreamAggregateProcessor) WithProcessorContext(pctx store.ProcessorContext) {
	p.pctx = pctx
}

func (p *StreamAggregateProcessor) Process(ctx context.Context, msg commtypes.Message) error {
	newMsg, err := p.ProcessAndReturn(ctx, msg)
	if err != nil {
		return err
	}
	return p.pipe.Forward(ctx, newMsg[0])
}

func (p *StreamAggregateProcessor) ProcessAndReturn(ctx context.Context, msg commtypes.Message) ([]commtypes.Message, error) {
	if msg.Key == nil || msg.Value == nil {
		log.Warn().Msgf("skipping record due to null key or value. key=%v, val=%v", msg.Key, msg.Value)
		return nil, nil
	}
	val, ok, err := p.store.Get(msg.Key)
	if err != nil {
		return nil, err
	}
	var oldAggTs commtypes.ValueTimestamp
	var oldAgg interface{}
	var newTs uint64
	if ok {
		oldAggTs = val.(commtypes.ValueTimestamp)
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
	err = p.store.Put(ctx, msg.Key, commtypes.ValueTimestamp{Timestamp: newTs, Value: newAgg})
	if err != nil {
		return nil, err
	}
	return []commtypes.Message{{Key: msg.Key, Value: newAgg, Timestamp: newTs}}, nil
}
