package processor

import (
	"context"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"sharedlog-stream/pkg/stream/processor/store"

	"github.com/rs/zerolog/log"
)

type StreamTableJoinProcessor struct {
	pipe      Pipe
	store     store.KeyValueStore
	pctx      store.ProcessorContext
	joiner    ValueJoinerWithKey
	storeName string
	leftJoin  bool
}

var _ = Processor(&StreamTableJoinProcessor{})

func NewStreamTableJoinProcessor(storeName string, joiner ValueJoinerWithKey) *StreamTableJoinProcessor {
	return &StreamTableJoinProcessor{
		storeName: storeName,
		joiner:    joiner,
	}
}

func (p *StreamTableJoinProcessor) WithPipe(pipe Pipe) {
	p.pipe = pipe
}

func (p *StreamTableJoinProcessor) WithProcessorContext(pctx store.ProcessorContext) {
	p.pctx = pctx
	p.store = p.pctx.GetKeyValueStore(p.storeName)
}

func (p *StreamTableJoinProcessor) Process(ctx context.Context, msg commtypes.Message) error {
	newMsg, err := p.ProcessAndReturn(ctx, msg)
	if err != nil {
		return err
	}
	if newMsg != nil {
		err := p.pipe.Forward(ctx, newMsg[0])
		if err != nil {
			return err
		}
	}
	return nil
}

func (p *StreamTableJoinProcessor) ProcessAndReturn(ctx context.Context, msg commtypes.Message) ([]commtypes.Message, error) {
	if msg.Key == nil || msg.Value == nil {
		log.Warn().Msgf("Skipping record due to null join key or value. key=%v, val=%v", msg.Key, msg.Value)
		return nil, nil
	}
	val2, ok, err := p.store.Get(msg.Key)
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
