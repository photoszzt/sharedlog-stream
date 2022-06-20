package processor

import (
	"context"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/store"
)

type TableFilterProcessor struct {
	pipe      Pipe
	pred      Predicate
	store     store.KeyValueStore
	filterNot bool
	name      string
}

var _ = Processor(&TableFilterProcessor{})

func NewTableFilterProcessor(name string, store store.KeyValueStore, pred Predicate, filterNot bool) *TableFilterProcessor {
	return &TableFilterProcessor{
		pred:      pred,
		filterNot: filterNot,
		store:     store,
		name:      name,
	}
}

func (p *TableFilterProcessor) WithPipe(pipe Pipe) {
	p.pipe = pipe
}

func (p *TableFilterProcessor) Name() string {
	return p.name
}

func (p *TableFilterProcessor) Process(ctx context.Context, msg commtypes.Message) error {
	ok, err := p.pred.Assert(&msg)
	if err != nil {
		return err
	}
	if ok != p.filterNot {
		err = p.store.Put(ctx, msg.Key, &commtypes.ValueTimestamp{Value: msg.Value, Timestamp: msg.Timestamp})
		if err != nil {
			return err
		}
		return p.pipe.Forward(ctx, msg)
	}
	return nil
}

func (p *TableFilterProcessor) ProcessAndReturn(ctx context.Context, msg commtypes.Message) ([]commtypes.Message, error) {
	panic("not implemented")
}
