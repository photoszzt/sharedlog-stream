package processor

type TableFilterProcessor struct {
	pipe          Pipe
	pctx          ProcessorContext
	pred          Predicate
	store         KeyValueStore
	filterNot     bool
	queryableName string
}

func NewTableFilterProcessor(pred Predicate, filterNot bool, queryableName string) *TableFilterProcessor {
	return &TableFilterProcessor{
		pred:          pred,
		filterNot:     filterNot,
		queryableName: queryableName,
	}
}

func (p *TableFilterProcessor) WithProcessorContext(pctx ProcessorContext) {
	p.pctx = pctx
}

func (p *TableFilterProcessor) WithPipe(pipe Pipe) {
	p.pipe = pipe
}

func (p *TableFilterProcessor) Process(msg Message) error {
	ok, err := p.pred.Assert(msg)
	if err != nil {
		return err
	}
	if ok != p.filterNot {
		if p.queryableName != "" {
			p.store.Put(msg.Key, &ValueTimestamp{Value: msg.Value, Timestamp: msg.Timestamp})
		}
		return p.pipe.Forward(msg)
	}
	return nil
}
