package processor

import (
	"context"
	"sharedlog-stream/pkg/commtypes"
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
	mapper Mapper
	name   string
}

var _ = Processor(&StreamMapProcessor{})

func NewStreamMapProcessor(name string, mapper Mapper) *StreamMapProcessor {
	return &StreamMapProcessor{
		mapper: mapper,
		name:   name,
	}
}

func (p *StreamMapProcessor) Name() string {
	return p.name
}

func (p *StreamMapProcessor) ProcessAndReturn(ctx context.Context, msg commtypes.Message) ([]commtypes.Message, error) {
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
	valueMapper ValueMapper
	name        string
}

var _ = Processor(&StreamMapValuesProcessor{})

func NewStreamMapValuesProcessor(mapper ValueMapper) *StreamMapValuesProcessor {
	return &StreamMapValuesProcessor{
		valueMapper: mapper,
	}
}

func (p *StreamMapValuesProcessor) Name() string {
	return p.name
}

func (p *StreamMapValuesProcessor) ProcessAndReturn(ctx context.Context, msg commtypes.Message) ([]commtypes.Message, error) {
	newV, err := p.valueMapper.MapValue(msg.Value)
	if err != nil {
		return nil, err
	}
	return []commtypes.Message{{Key: msg.Key, Value: newV, Timestamp: msg.Timestamp}}, nil
}

type StreamMapValuesWithKeyProcessor struct {
	valueWithKeyMapper Mapper
	name               string
}

func NewStreamMapValuesWithKeyProcessor(mapper Mapper) Processor {
	return &StreamMapValuesWithKeyProcessor{
		valueWithKeyMapper: mapper,
	}
}

func (p *StreamMapValuesWithKeyProcessor) Name() string {
	return p.name
}

func (p *StreamMapValuesWithKeyProcessor) ProcessAndReturn(ctx context.Context, msg commtypes.Message) ([]commtypes.Message, error) {
	newMsg, err := p.valueWithKeyMapper.Map(msg)
	if err != nil {
		return nil, err
	}
	return []commtypes.Message{{Key: msg.Key, Value: newMsg.Value, Timestamp: msg.Timestamp}}, nil
}

type SelectKeyMapper interface {
	SelectKey(msg commtypes.Message) (interface{}, error)
}
type SelectKeyFunc func(msg commtypes.Message) (interface{}, error)

var _ = SelectKeyMapper(SelectKeyFunc(nil))

func (fn SelectKeyFunc) SelectKey(msg commtypes.Message) (interface{}, error) {
	return fn(msg)
}

type StreamSelectKeyProcessor struct {
	selectKey SelectKeyMapper
	name      string
}

func NewStreamSelectKeyProcessor(name string, keySelector SelectKeyMapper) Processor {
	return &StreamSelectKeyProcessor{
		name:      name,
		selectKey: keySelector,
	}
}

func (p *StreamSelectKeyProcessor) Name() string {
	return p.name
}

func (p *StreamSelectKeyProcessor) ProcessAndReturn(ctx context.Context, msg commtypes.Message) ([]commtypes.Message, error) {
	newKey, err := p.selectKey.SelectKey(msg)
	if err != nil {
		return nil, err
	}
	return []commtypes.Message{{Key: newKey, Value: msg.Value, Timestamp: msg.Timestamp}}, nil
}
