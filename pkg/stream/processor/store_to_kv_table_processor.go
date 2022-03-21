package processor

import (
	"context"
	"os"
	"sharedlog-stream/pkg/debug"
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
	err := p.store.Put(ctx, msg.Key, commtypes.ValueTimestamp{Value: msg.Value, Timestamp: msg.Timestamp})
	debug.Fprintf(os.Stderr, "store to kv store, k: %v, v: %v, ts: %v\n", msg.Key, msg.Value, msg.Timestamp)
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

func ToMongoDBKVTable(ctx context.Context,
	dbName string,
	mongoAddr string,
	keySerde commtypes.Serde,
	valSerde commtypes.Serde,
) (*MeteredProcessor, store.KeyValueStore, error) {
	mkvs, err := store.NewMongoDBKeyValueStore(ctx, &store.MongoDBConfig{
		Addr:           mongoAddr,
		CollectionName: dbName,
		DBName:         dbName,
		KeySerde:       keySerde,
		ValueSerde:     valSerde,
	})
	if err != nil {
		return nil, nil, err
	}
	return NewMeteredProcessor(NewStoreToKVTableProcessor(mkvs)), mkvs, nil
}
