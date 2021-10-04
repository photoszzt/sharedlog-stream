package processor

type Mapper interface {
	Map(Message) (Message, error)
}
type MapperFunc func(Message) (Message, error)

var _ = (Mapper)(MapperFunc(nil))

func (fn MapperFunc) Map(msg Message) (Message, error) {
	return fn(msg)
}

type StreamMapProcessor struct {
	pipe   Pipe
	mapper Mapper
	pctx   ProcessorContext
}

func NewStreamMapProcessor(mapper Mapper) *StreamMapProcessor {
	return &StreamMapProcessor{
		mapper: mapper,
	}
}

func (p *StreamMapProcessor) WithPipe(pipe Pipe) {
	p.pipe = pipe
}

func (p *StreamMapProcessor) WithProcessorContext(pctx ProcessorContext) {
	p.pctx = pctx
}

func (p *StreamMapProcessor) Process(msg Message) error {
	m, err := p.mapper.Map(msg)
	if err != nil {
		return err
	}
	m.Timestamp = msg.Timestamp
	return p.pipe.Forward(m)
}

func (p *StreamMapProcessor) ProcessAndReturn(msg Message) (*Message, error) {
	m, err := p.mapper.Map(msg)
	if err != nil {
		return nil, err
	}
	m.Timestamp = msg.Timestamp
	return &m, nil
}

type ValueMapper interface {
	MapValue(value interface{}) (interface{}, error)
}
type ValueMapperFunc func(interface{}) (interface{}, error)

var _ = ValueMapper(ValueMapperFunc(nil))

func (fn ValueMapperFunc) MapValue(value interface{}) (interface{}, error) {
	return fn(value)
}

type StreamMapValuesProcessor struct {
	pipe        Pipe
	valueMapper ValueMapper
	pctx        ProcessorContext
}

func NewStreamMapValuesProcessor(mapper ValueMapper) *StreamMapValuesProcessor {
	return &StreamMapValuesProcessor{
		valueMapper: mapper,
	}
}

func (p *StreamMapValuesProcessor) WithPipe(pipe Pipe) {
	p.pipe = pipe
}

func (p *StreamMapValuesProcessor) WithProcessorContext(pctx ProcessorContext) {
	p.pctx = pctx
}

func (p *StreamMapValuesProcessor) Process(msg Message) error {
	newV, err := p.valueMapper.MapValue(msg.Value)
	if err != nil {
		return err
	}
	return p.pipe.Forward(Message{Key: msg.Key, Value: newV, Timestamp: msg.Timestamp})
}

func (p *StreamMapValuesProcessor) ProcessAndReturn(msg Message) (*Message, error) {
	newV, err := p.valueMapper.MapValue(msg.Value)
	if err != nil {
		return nil, err
	}
	return &Message{Key: msg.Key, Value: newV, Timestamp: msg.Timestamp}, nil
}

type StreamMapValuesWithKeyProcessor struct {
	pipe               Pipe
	valueWithKeyMapper Mapper
	pctx               ProcessorContext
}

func NewStreamMapValuesWithKeyProcessor(mapper Mapper) Processor {
	return &StreamMapValuesWithKeyProcessor{
		valueWithKeyMapper: mapper,
	}
}

func (p *StreamMapValuesWithKeyProcessor) WithPipe(pipe Pipe) {
	p.pipe = pipe
}

func (p *StreamMapValuesWithKeyProcessor) WithProcessorContext(pctx ProcessorContext) {
	p.pctx = pctx
}

func (p *StreamMapValuesWithKeyProcessor) Process(msg Message) error {
	newMsg, err := p.ProcessAndReturn(msg)
	if err != nil {
		return err
	}
	return p.pipe.Forward(*newMsg)
}

func (p *StreamMapValuesWithKeyProcessor) ProcessAndReturn(msg Message) (*Message, error) {
	newMsg, err := p.valueWithKeyMapper.Map(msg)
	if err != nil {
		return nil, err
	}
	return &Message{Key: msg.Key, Value: newMsg.Value, Timestamp: msg.Timestamp}, nil
}
