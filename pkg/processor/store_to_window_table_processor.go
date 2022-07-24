package processor

import (
	"context"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/concurrent_skiplist"
	"sharedlog-stream/pkg/store"
	"sharedlog-stream/pkg/utils"
)

type StoreToWindowTableProcessor struct {
	store      store.CoreWindowStore
	name       string
	observedTs int64
}

var _ = Processor(&StoreToWindowTableProcessor{})

func NewStoreToWindowTableProcessor(store store.CoreWindowStore) *StoreToWindowTableProcessor {
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
	if !utils.IsNil(msg.Key) {
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
) (*MeteredProcessor, store.CoreWindowStore, error) {
	store := store.NewInMemoryWindowStore(
		storeName,
		joinWindow.MaxSize()+joinWindow.GracePeriodMs(),
		joinWindow.MaxSize(),
		true, compare)
	toTableProc := NewMeteredProcessor(NewStoreToWindowTableProcessor(store))
	return toTableProc, store, nil
}
