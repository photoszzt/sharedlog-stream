package processor

import (
	"context"
	"sharedlog-stream/pkg/concurrent_skiplist"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"sharedlog-stream/pkg/stream/processor/store"
)

type StoreToWindowTableProcessor struct {
	pctx       store.StoreContext
	pipe       Pipe
	store      store.WindowStore
	observedTs int64
}

var _ = Processor(&StoreToWindowTableProcessor{})

func (p *StoreToWindowTableProcessor) WithProcessorContext(pctx store.StoreContext) {
	p.pctx = pctx
}

func (p *StoreToWindowTableProcessor) WithPipe(pipe Pipe) {
	p.pipe = pipe
}

func NewStoreToWindowTableProcessor(store store.WindowStore) *StoreToWindowTableProcessor {
	return &StoreToWindowTableProcessor{
		store: store,
	}
}

func (p *StoreToWindowTableProcessor) Process(ctx context.Context, msg commtypes.Message) error {
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

func (p *StoreToWindowTableProcessor) ProcessAndReturn(ctx context.Context, msg commtypes.Message) ([]commtypes.Message, error) {
	val := msg.Value.(commtypes.StreamTimeExtractor)
	ts, err := val.ExtractStreamTime()
	if err != nil {
		return nil, err
	}
	if ts > p.observedTs {
		p.observedTs = ts
	}
	if msg.Key != nil {
		err := p.store.Put(ctx, msg.Key, msg.Value, p.observedTs)
		if err != nil {
			return nil, err
		}
		return []commtypes.Message{msg}, nil
	}
	return nil, nil
}

func ToInMemWindowTable(
	storeName string,
	joinWindow *JoinWindows,
	compare concurrent_skiplist.CompareFunc,
) (*MeteredProcessor, store.WindowStore, error) {
	store := store.NewInMemoryWindowStore(
		storeName,
		joinWindow.GracePeriodMs(),
		joinWindow.MaxSize(),
		true, compare)
	toTableProc := NewMeteredProcessor(NewStoreToWindowTableProcessor(store))
	return toTableProc, store, nil
}
