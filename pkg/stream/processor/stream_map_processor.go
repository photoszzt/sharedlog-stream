package processor

import (
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"sharedlog-stream/pkg/stream/processor/store"
)

type Mapper interface {
	Map(commtypes.Message) (commtypes.Message, error)
}
type MapperFunc func(commtypes.Message) (commtypes.Message, error)

var _ = (Mapper)(MapperFunc(nil))

func (fn MapperFunc) Map(msg commtypes.Message) (commtypes.Message, error) {
	return fn(msg)
}

type StreamMapProcessor struct {
	pipe   Pipe
	mapper Mapper
	pctx   store.ProcessorContext
}

var _ = Processor(&StreamMapProcessor{})

func NewStreamMapProcessor(mapper Mapper) *StreamMapProcessor {
	return &StreamMapProcessor{
		mapper: mapper,
	}
}

func (p *StreamMapProcessor) WithPipe(pipe Pipe) {
	p.pipe = pipe
}

func (p *StreamMapProcessor) WithProcessorContext(pctx store.ProcessorContext) {
	p.pctx = pctx
}

func (p *StreamMapProcessor) Process(msg commtypes.Message) error {
	m, err := p.mapper.Map(msg)
	if err != nil {
		return err
	}
	m.Timestamp = msg.Timestamp
	return p.pipe.Forward(m)
}

func (p *StreamMapProcessor) ProcessAndReturn(msg commtypes.Message) ([]commtypes.Message, error) {
	m, err := p.mapper.Map(msg)
	if err != nil {
		return nil, err
	}
	m.Timestamp = msg.Timestamp
	return []commtypes.Message{m}, nil
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
	pctx        store.ProcessorContext
}

var _ = Processor(&StreamMapValuesProcessor{})

func NewStreamMapValuesProcessor(mapper ValueMapper) *StreamMapValuesProcessor {
	return &StreamMapValuesProcessor{
		valueMapper: mapper,
	}
}

func (p *StreamMapValuesProcessor) WithPipe(pipe Pipe) {
	p.pipe = pipe
}

func (p *StreamMapValuesProcessor) WithProcessorContext(pctx store.ProcessorContext) {
	p.pctx = pctx
}

func (p *StreamMapValuesProcessor) Process(msg commtypes.Message) error {
	newV, err := p.valueMapper.MapValue(msg.Value)
	if err != nil {
		return err
	}
	return p.pipe.Forward(commtypes.Message{Key: msg.Key, Value: newV, Timestamp: msg.Timestamp})
}

func (p *StreamMapValuesProcessor) ProcessAndReturn(msg commtypes.Message) ([]commtypes.Message, error) {
	newV, err := p.valueMapper.MapValue(msg.Value)
	if err != nil {
		return nil, err
	}
	return []commtypes.Message{commtypes.Message{Key: msg.Key, Value: newV, Timestamp: msg.Timestamp}}, nil
}

type StreamMapValuesWithKeyProcessor struct {
	pipe               Pipe
	valueWithKeyMapper Mapper
	pctx               store.ProcessorContext
}

func NewStreamMapValuesWithKeyProcessor(mapper Mapper) Processor {
	return &StreamMapValuesWithKeyProcessor{
		valueWithKeyMapper: mapper,
	}
}

func (p *StreamMapValuesWithKeyProcessor) WithPipe(pipe Pipe) {
	p.pipe = pipe
}

func (p *StreamMapValuesWithKeyProcessor) WithProcessorContext(pctx store.ProcessorContext) {
	p.pctx = pctx
}

func (p *StreamMapValuesWithKeyProcessor) Process(msg commtypes.Message) error {
	newMsg, err := p.ProcessAndReturn(msg)
	if err != nil {
		return err
	}
	return p.pipe.Forward(newMsg[0])
}

func (p *StreamMapValuesWithKeyProcessor) ProcessAndReturn(msg commtypes.Message) ([]commtypes.Message, error) {
	newMsg, err := p.valueWithKeyMapper.Map(msg)
	if err != nil {
		return nil, err
	}
	return []commtypes.Message{commtypes.Message{Key: msg.Key, Value: newMsg.Value, Timestamp: msg.Timestamp}}, nil
}
