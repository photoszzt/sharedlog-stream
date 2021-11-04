package processor

import (
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"sharedlog-stream/pkg/stream/processor/store"
)

type TableFilterProcessor struct {
	pipe          Pipe
	pctx          store.ProcessorContext
	pred          Predicate
	store         store.KeyValueStore
	queryableName string
	filterNot     bool
}

var _ = Processor(&TableFilterProcessor{})

func NewTableFilterProcessor(pred Predicate, filterNot bool, queryableName string) *TableFilterProcessor {
	return &TableFilterProcessor{
		pred:          pred,
		filterNot:     filterNot,
		queryableName: queryableName,
	}
}

func (p *TableFilterProcessor) WithProcessorContext(pctx store.ProcessorContext) {
	p.pctx = pctx
	if p.queryableName != "" {
		p.store = p.pctx.GetKeyValueStore(p.queryableName)
	}
}

func (p *TableFilterProcessor) WithPipe(pipe Pipe) {
	p.pipe = pipe
}

func (p *TableFilterProcessor) Process(msg commtypes.Message) error {
	ok, err := p.pred.Assert(&msg)
	if err != nil {
		return err
	}
	if ok != p.filterNot {
		if p.queryableName != "" {
			err = p.store.Put(msg.Key, &commtypes.ValueTimestamp{Value: msg.Value, Timestamp: msg.Timestamp})
			if err != nil {
				return err
			}
		}
		return p.pipe.Forward(msg)
	}
	return nil
}

func (p *TableFilterProcessor) ProcessAndReturn(msg commtypes.Message) ([]commtypes.Message, error) {
	panic("not implemented")
}
