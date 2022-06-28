package processor

import (
	"context"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/concurrent_skiplist"
	"sharedlog-stream/pkg/store"

	"go.mongodb.org/mongo-driver/mongo"
)

type StoreToWindowTableProcessor struct {
	store      store.WindowStore
	name       string
	observedTs int64
}

var _ = Processor(&StoreToWindowTableProcessor{})

func NewStoreToWindowTableProcessor(store store.WindowStore) *StoreToWindowTableProcessor {
	return &StoreToWindowTableProcessor{
		store: store,
		name:  "StoreTo" + store.Name(),
	}
}

func (p *StoreToWindowTableProcessor) Name() string {
	return p.name
}

func (p *StoreToWindowTableProcessor) ProcessAndReturn(ctx context.Context, msg commtypes.Message) ([]commtypes.Message, error) {
	if msg.Timestamp > p.observedTs {
		p.observedTs = msg.Timestamp
	}
	if msg.Key != nil {
		err := p.store.Put(ctx, msg.Key, msg.Value, p.observedTs)
		if err != nil {
			return nil, err
		}
	}
	return []commtypes.Message{msg}, nil
}

// for test
func ToInMemWindowTable(
	storeName string,
	joinWindow *JoinWindows,
	compare concurrent_skiplist.CompareFunc,
) (*MeteredProcessor, store.WindowStore, error) {
	store := store.NewInMemoryWindowStore(
		storeName,
		joinWindow.MaxSize()+joinWindow.GracePeriodMs(),
		joinWindow.MaxSize(),
		true, compare)
	toTableProc := NewMeteredProcessor(NewStoreToWindowTableProcessor(store))
	return toTableProc, store, nil
}

func ToMongoDBWindowTable(
	ctx context.Context,
	storeName string,
	client *mongo.Client,
	joinWindow *JoinWindows,
	keySerde commtypes.Serde,
	valSerde commtypes.Serde,
) (*MeteredProcessor, store.WindowStore, error) {
	mkvs, err := store.NewMongoDBKeyValueStore(ctx, &store.MongoDBConfig{
		Client:         client,
		CollectionName: storeName,
		DBName:         storeName,
		KeySerde:       nil,
		ValueSerde:     nil,
	})
	if err != nil {
		return nil, nil, err
	}
	byteStore, err := store.NewMongoDBSegmentedBytesStore(ctx, storeName,
		joinWindow.MaxSize()+joinWindow.GracePeriodMs(), &store.WindowKeySchema{}, mkvs)
	if err != nil {
		return nil, nil, err
	}
	wstore := store.NewSegmentedWindowStore(byteStore, true, joinWindow.MaxSize(), keySerde, valSerde)
	toTableProc := NewMeteredProcessor(NewStoreToWindowTableProcessor(wstore))
	return toTableProc, wstore, nil
}

func CreateMongoDBWindoeTable(
	ctx context.Context,
	storeName string,
	client *mongo.Client,
	retention int64,
	windowSize int64,
	keySerde commtypes.Serde,
	valSerde commtypes.Serde,
) (store.WindowStore, error) {
	mkvs, err := store.NewMongoDBKeyValueStore(ctx, &store.MongoDBConfig{
		Client:         client,
		CollectionName: storeName,
		DBName:         storeName,
		KeySerde:       nil,
		ValueSerde:     nil,
	})
	if err != nil {
		return nil, err
	}
	byteStore, err := store.NewMongoDBSegmentedBytesStore(ctx, storeName,
		retention, &store.WindowKeySchema{}, mkvs)
	if err != nil {
		return nil, err
	}
	wstore := store.NewSegmentedWindowStore(byteStore, true, windowSize, keySerde, valSerde)
	return wstore, nil
}
