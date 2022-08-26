package processor

import (
	"context"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/optional"
	"sharedlog-stream/pkg/store"
	"sharedlog-stream/pkg/utils"
)

type StoreToWindowTableProcessor struct {
	store store.CoreWindowStore
	name  string
	BaseProcessor
	observedTs int64
}

var _ = Processor(&StoreToWindowTableProcessor{})

func NewStoreToWindowTableProcessor(store store.CoreWindowStore) *StoreToWindowTableProcessor {
	p := &StoreToWindowTableProcessor{
		store:         store,
		name:          "StoreTo" + store.Name(),
		BaseProcessor: BaseProcessor{},
	}
	p.BaseProcessor.ProcessingFunc = p.ProcessAndReturn
	return p
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

type StoreToWindowTableProcessorG[K, V any] struct {
	store store.CoreWindowStoreG[K, V]
	name  string
	BaseProcessor
	observedTs int64
}

var _ = Processor(&StoreToWindowTableProcessor{})

func NewStoreToWindowTableProcessorG[K, V any](store store.CoreWindowStoreG[K, V]) *StoreToWindowTableProcessorG[K, V] {
	p := &StoreToWindowTableProcessorG[K, V]{
		store:         store,
		name:          "StoreTo" + store.Name(),
		BaseProcessor: BaseProcessor{},
	}
	p.BaseProcessor.ProcessingFunc = p.ProcessAndReturn
	return p
}

func (p *StoreToWindowTableProcessorG[K, V]) Name() string { return p.name }
func (p *StoreToWindowTableProcessorG[K, V]) ProcessAndReturn(ctx context.Context, msg commtypes.Message) ([]commtypes.Message, error) {
	if msg.Timestamp > p.observedTs {
		p.observedTs = msg.Timestamp
	}
	if !utils.IsNil(msg.Key) {
		err := p.store.Put(ctx, msg.Key.(K), optional.Some(msg.Value.(V)), p.observedTs)
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
	compare store.CompareFunc,
) (*MeteredProcessor, store.CoreWindowStore, error) {
	store := store.NewInMemoryWindowStore(
		storeName,
		joinWindow.MaxSize()+joinWindow.GracePeriodMs(),
		joinWindow.MaxSize(), true, compare)
	toTableProc := NewMeteredProcessor(NewStoreToWindowTableProcessor(store))
	return toTableProc, store, nil
}

func ToInMemSkipMapWindowTable[K, V any](
	storeName string,
	joinWindow *JoinWindows,
	compare store.CompareFuncG[K],
) (*MeteredProcessor, store.CoreWindowStoreG[K, V], error) {
	store := store.NewInMemorySkipMapWindowStore[K, V](
		storeName,
		joinWindow.MaxSize()+joinWindow.GracePeriodMs(),
		joinWindow.MaxSize(), true, compare)
	toTableProc := NewMeteredProcessor(NewStoreToWindowTableProcessorG[K, V](store))
	return toTableProc, store, nil
}
