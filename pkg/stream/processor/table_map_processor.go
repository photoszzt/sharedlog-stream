package processor

type TableMapValuesProcessor struct {
	pipe          Pipe
	pctx          ProcessorContext
	store         KeyValueStore
	queryableName string
	valueMapper   ValueMapper
}

func NewTableMapValuesProcessor(mapper ValueMapper, queryableName string) Processor {
	return &TableMapValuesProcessor{
		valueMapper:   mapper,
		queryableName: queryableName,
	}
}

func (p *TableMapValuesProcessor) WithPipe(pipe Pipe) {
	p.pipe = pipe
}

func (p *TableMapValuesProcessor) WithProcessorContext(pctx ProcessorContext) {
	p.pctx = pctx
	if p.queryableName != "" {
		p.store = p.pctx.GetKeyValueStore(p.queryableName)
	}
}

func (p *TableMapValuesProcessor) Process(msg Message) error {
	newV, err := p.valueMapper.MapValue(msg.Value)
	if err != nil {
		return err
	}
	newMsg := Message{Key: msg.Key, Value: newV, Timestamp: msg.Timestamp}
	if p.queryableName != "" {
		p.store.Put(msg.Key, &ValueTimestamp{Value: newMsg.Value, Timestamp: newMsg.Timestamp})
	}
	return p.pipe.Forward(newMsg)
}
