package processor

import (
	"context"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"sharedlog-stream/pkg/stream/processor/store"

	"github.com/rs/zerolog/log"
)

type TableJoinProcessor struct {
	pipe      Pipe
	store     store.KeyValueStore
	pctx      store.StoreContext
	joiner    ValueJoinerWithKey
	storeName string
	leftJoin  bool
}

var _ = Processor(&TableJoinProcessor{})

func NewTableJoinProcessor(storeName string, store store.KeyValueStore, joiner ValueJoinerWithKey) *TableJoinProcessor {
	return &TableJoinProcessor{
		storeName: storeName,
		joiner:    joiner,
		store:     store,
	}
}

func (p *TableJoinProcessor) WithPipe(pipe Pipe) {
	p.pipe = pipe
}

func (p *TableJoinProcessor) WithProcessorContext(pctx store.StoreContext) {
	p.pctx = pctx
	p.store = p.pctx.GetKeyValueStore(p.storeName)
}

func (p *TableJoinProcessor) Process(ctx context.Context, msg commtypes.Message) error {
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

func (p *TableJoinProcessor) ProcessAndReturn(ctx context.Context, msg commtypes.Message) ([]commtypes.Message, error) {
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
