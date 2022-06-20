package processor

import (
	"context"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/store"
)

type TableMapValuesProcessor struct {
	store       store.KeyValueStore
	valueMapper ValueMapper
	name        string
}

func NewTableMapValuesProcessor(name string, mapper ValueMapper, store store.KeyValueStore) Processor {
	return &TableMapValuesProcessor{
		valueMapper: mapper,
		store:       store,
		name:        name,
	}
}

func (p *TableMapValuesProcessor) Name() string {
	return p.name
}

func (p *TableMapValuesProcessor) ProcessAndReturn(ctx context.Context, msg commtypes.Message) ([]commtypes.Message, error) {
	newV, err := p.valueMapper.MapValue(msg.Value)
	if err != nil {
		return nil, err
	}
	newMsg := commtypes.Message{Key: msg.Key, Value: newV, Timestamp: msg.Timestamp}
	err = p.store.Put(ctx, msg.Key, &commtypes.ValueTimestamp{Value: newMsg.Value, Timestamp: newMsg.Timestamp})
	if err != nil {
		return nil, err
	}
	return []commtypes.Message{newMsg}, nil
}
