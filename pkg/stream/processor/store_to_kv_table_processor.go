package processor

import (
	"context"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"sharedlog-stream/pkg/stream/processor/store"
	"sharedlog-stream/pkg/treemap"
)

type StoreToKVTableProcessor struct {
	pctx  store.StoreContext
	pipe  Pipe
	store store.KeyValueStore
}

var _ = Processor(&StoreToKVTableProcessor{})

func (p *StoreToKVTableProcessor) WithProcessorContext(pctx store.StoreContext) {
	p.pctx = pctx
}

func (p *StoreToKVTableProcessor) WithPipe(pipe Pipe) {
	p.pipe = pipe
}

func NewStoreToKVTableProcessor(store store.KeyValueStore) *StoreToKVTableProcessor {
	return &StoreToKVTableProcessor{
		store: store,
	}
}

func (p *StoreToKVTableProcessor) Process(ctx context.Context, msg commtypes.Message) error {
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

func (p *StoreToKVTableProcessor) ProcessAndReturn(ctx context.Context, msg commtypes.Message) ([]commtypes.Message, error) {
	err := p.store.Put(ctx, msg.Key, msg.Value)
	if err != nil {
		return nil, err
	}
	return []commtypes.Message{msg}, nil
}

func ToInMemKVTable(storeName string, compare func(a, b treemap.Key) int) (*MeteredProcessor, store.KeyValueStore, error) {
	store := store.NewInMemoryKeyValueStore(storeName, compare)
	toTableProc := NewMeteredProcessor(NewStoreToKVTableProcessor(store))
	return toTableProc, store, nil
}
