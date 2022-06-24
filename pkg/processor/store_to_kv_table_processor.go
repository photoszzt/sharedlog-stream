package processor

import (
	"context"
	"fmt"
	"os"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/debug"
	"sharedlog-stream/pkg/store"
	"sharedlog-stream/pkg/treemap"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
)

type StoreToKVTableProcessor struct {
	store store.KeyValueStore
	name  string
}

var _ = Processor(&StoreToKVTableProcessor{})

func NewStoreToKVTableProcessor(store store.KeyValueStore) *StoreToKVTableProcessor {
	return &StoreToKVTableProcessor{
		store: store,
		name:  "StoreTo" + store.Name(),
	}
}

func (p *StoreToKVTableProcessor) Name() string {
	return p.name
}

func (p *StoreToKVTableProcessor) ProcessAndReturn(ctx context.Context, msg commtypes.Message) ([]commtypes.Message, error) {
	valTmp, ok, err := p.store.Get(ctx, msg.Key)
	if err != nil {
		return nil, err
	}
	if ok {
		oldAggTs := valTmp.(commtypes.ValueTimestamp)
		if msg.Timestamp < oldAggTs.Timestamp {
			fmt.Fprintf(os.Stderr, "Detected out-of-order table update for %s, old ts=[%d] new ts=[%d]\n",
				p.store.Name(), oldAggTs.Timestamp, msg.Timestamp)
		}
	}
	err = p.store.Put(ctx, msg.Key, commtypes.ValueTimestamp{Value: msg.Value, Timestamp: msg.Timestamp})
	debug.Fprintf(os.Stderr, "store to kv store, k: %v, v: %v, ts: %v\n", msg.Key, msg.Value, msg.Timestamp)
	if err != nil {
		return nil, err
	}
	return []commtypes.Message{msg}, nil
}

func ToInMemKVTable(storeName string, compare func(a, b treemap.Key) int) (
	*MeteredProcessor, store.KeyValueStore,
) {
	s := store.NewInMemoryKeyValueStore(storeName, compare)
	toTableProc := NewMeteredProcessor(NewStoreToKVTableProcessor(s))
	return toTableProc, s
}

func ToMongoDBKVTable(ctx context.Context,
	dbName string,
	client *mongo.Client,
	keySerde commtypes.Serde,
	valSerde commtypes.Serde,
	warmup time.Duration,
) (*MeteredProcessor, store.KeyValueStore, error) {
	mkvs, err := store.NewMongoDBKeyValueStore(ctx, &store.MongoDBConfig{
		Client:         client,
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
