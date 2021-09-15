package processor

type Mapper interface {
	Map(Message) (Message, error)
}
type MapperFunc func(Message) (Message, error)

var _ = (Mapper)(MapperFunc(nil))

func (fn MapperFunc) Map(msg Message) (Message, error) {
	return fn(msg)
}

type MapProcessor struct {
	pipe   Pipe
	mapper Mapper
	pctx   ProcessorContext
}

func NewMapProcessor(mapper Mapper) Processor {
	return &MapProcessor{
		mapper: mapper,
	}
}

func (p *MapProcessor) WithPipe(pipe Pipe) {
	p.pipe = pipe
}

func (p *MapProcessor) WithProcessorContext(pctx ProcessorContext) {
	p.pctx = pctx
}

func (p *MapProcessor) Process(msg Message) error {
	m, err := p.mapper.Map(msg)
	if err != nil {
		return err
	}
	m.Timestamp = msg.Timestamp
	return p.pipe.Forward(m)
}

type ValueMapper interface {
	MapValue(value interface{}) (interface{}, error)
}
type ValueMapperFunc func(interface{}) (interface{}, error)

var _ = ValueMapper(ValueMapperFunc(nil))

func (fn ValueMapperFunc) MapValue(value interface{}) (interface{}, error) {
	return fn(value)
}

type MapValuesProcessor struct {
	pipe        Pipe
	valueMapper ValueMapper
	pctx        ProcessorContext
}

func NewMapValuesProcessor(mapper ValueMapper) Processor {
	return &MapValuesProcessor{
		valueMapper: mapper,
	}
}

func (p *MapValuesProcessor) WithPipe(pipe Pipe) {
	p.pipe = pipe
}

func (p *MapValuesProcessor) WithProcessorContext(pctx ProcessorContext) {
	p.pctx = pctx
}

func (p *MapValuesProcessor) Process(msg Message) error {
	newV, err := p.valueMapper.MapValue(msg.Value)
	if err != nil {
		return err
	}
	return p.pipe.Forward(Message{Key: msg.Key, Value: newV, Timestamp: msg.Timestamp})
}

type MapValuesWithKeyProcessor struct {
	pipe               Pipe
	valueWithKeyMapper Mapper
	pctx               ProcessorContext
}

func NewMapValuesWithKeyProcessor(mapper Mapper) Processor {
	return &MapValuesWithKeyProcessor{
		valueWithKeyMapper: mapper,
	}
}

func (p *MapValuesWithKeyProcessor) WithPipe(pipe Pipe) {
	p.pipe = pipe
}

func (p *MapValuesWithKeyProcessor) WithProcessorContext(pctx ProcessorContext) {
	p.pctx = pctx
}

func (p *MapValuesWithKeyProcessor) Process(msg Message) error {
	newMsg, err := p.valueWithKeyMapper.Map(msg)
	if err != nil {
		return err
	}
	return p.pipe.Forward(Message{Key: msg.Key, Value: newMsg.Value, Timestamp: msg.Timestamp})
}
