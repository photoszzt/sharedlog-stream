package processor

import (
	"context"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"sharedlog-stream/pkg/stream/processor/store"
)

type TableMapValuesProcessor struct {
	pipe          Pipe
	pctx          store.StoreContext
	store         store.KeyValueStore
	valueMapper   ValueMapper
	queryableName string
}

func NewTableMapValuesProcessor(mapper ValueMapper, queryableName string) Processor {
	return &TableMapValuesProcessor{
		valueMapper:   mapper,
		queryableName: queryableName,
	}
}

func (p *TableMapValuesProcessor) WithPipe(pipe Pipe) {
	p.pipe = pipe
}

func (p *TableMapValuesProcessor) WithProcessorContext(pctx store.StoreContext) {
	p.pctx = pctx
	if p.queryableName != "" {
		p.store = p.pctx.GetKeyValueStore(p.queryableName)
	}
}

func (p *TableMapValuesProcessor) Process(ctx context.Context, msg commtypes.Message) error {
	newMsg, err := p.ProcessAndReturn(ctx, msg)
	if err != nil {
		return err
	}
	return p.pipe.Forward(ctx, newMsg[0])
}

func (p *TableMapValuesProcessor) ProcessAndReturn(ctx context.Context, msg commtypes.Message) ([]commtypes.Message, error) {
	newV, err := p.valueMapper.MapValue(msg.Value)
	if err != nil {
		return nil, err
	}
	newMsg := commtypes.Message{Key: msg.Key, Value: newV, Timestamp: msg.Timestamp}
	if p.queryableName != "" {
		err = p.store.Put(ctx, msg.Key, &commtypes.ValueTimestamp{Value: newMsg.Value, Timestamp: newMsg.Timestamp})
		if err != nil {
			return nil, err
		}
	}
	return []commtypes.Message{newMsg}, nil
}
