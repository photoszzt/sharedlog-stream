package processor

type TableFilterProcessor struct {
	pipe          Pipe
	pctx          ProcessorContext
	pred          Predicate
	store         KeyValueStore
	filterNot     bool
	queryableName string
}

var _ = Processor(&TableFilterProcessor{})

func NewTableFilterProcessor(pred Predicate, filterNot bool, queryableName string) *TableFilterProcessor {
	return &TableFilterProcessor{
		pred:          pred,
		filterNot:     filterNot,
		queryableName: queryableName,
	}
}

func (p *TableFilterProcessor) WithProcessorContext(pctx ProcessorContext) {
	p.pctx = pctx
	if p.queryableName != "" {
		p.store = p.pctx.GetKeyValueStore(p.queryableName)
	}
}

func (p *TableFilterProcessor) WithPipe(pipe Pipe) {
	p.pipe = pipe
}

func (p *TableFilterProcessor) Process(msg Message) error {
	ok, err := p.pred.Assert(&msg)
	if err != nil {
		return err
	}
	if ok != p.filterNot {
		if p.queryableName != "" {
			err = p.store.Put(msg.Key, &ValueTimestamp{Value: msg.Value, Timestamp: msg.Timestamp})
			if err != nil {
				return err
			}
		}
		return p.pipe.Forward(msg)
	}
	return nil
}

func (p *TableFilterProcessor) ProcessAndReturn(msg Message) ([]Message, error) {
	panic("not implemented")
}
