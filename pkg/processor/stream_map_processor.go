package processor

import (
	"context"
	"sharedlog-stream/pkg/commtypes"
)

type Mapper interface {
	Map(key, value interface{}) (interface{} /* key */, interface{} /* value */, error)
}
type MapperFunc func(key, value interface{}) (interface{} /* key */, interface{} /* value */, error)

var _ = (Mapper)(MapperFunc(nil))

func (fn MapperFunc) Map(key, value interface{}) (interface{} /* key */, interface{} /* value */, error) {
	return fn(key, value)
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
	newK, newV, err := p.mapper.Map(msg.Key, msg.Value)
	if err != nil {
		return nil, err
	}
	return []commtypes.Message{{Key: newK, Value: newV, Timestamp: msg.Timestamp}}, nil
}

type ValueMapperWithKey interface {
	MapValue(key interface{}, value interface{}) (interface{}, error)
}
type ValueMapperWithKeyFunc func(key interface{}, value interface{}) (interface{}, error)

var _ = ValueMapperWithKey(ValueMapperWithKeyFunc(nil))

func (fn ValueMapperWithKeyFunc) MapValue(key interface{}, value interface{}) (interface{}, error) {
	return fn(key, value)
}

type StreamMapValuesProcessor struct {
	valueMapperWithKey ValueMapperWithKey
	name               string
}

var _ = Processor(&StreamMapValuesProcessor{})

func NewStreamMapValuesProcessor(name string, mapper ValueMapperWithKey) *StreamMapValuesProcessor {
	return &StreamMapValuesProcessor{
		valueMapperWithKey: mapper,
		name:               name,
	}
}

func (p *StreamMapValuesProcessor) Name() string {
	return p.name
}

func (p *StreamMapValuesProcessor) ProcessAndReturn(ctx context.Context, msg commtypes.Message) ([]commtypes.Message, error) {
	newV, err := p.valueMapperWithKey.MapValue(msg.Key, msg.Value)
	if err != nil {
		return nil, err
	}
	return []commtypes.Message{{Key: msg.Key, Value: newV, Timestamp: msg.Timestamp}}, nil
}

type SelectKeyMapper interface {
	SelectKey(key, value interface{}) (interface{}, error)
}
type SelectKeyFunc func(key, value interface{}) (interface{}, error)

var _ = SelectKeyMapper(SelectKeyFunc(nil))

func (fn SelectKeyFunc) SelectKey(key, value interface{}) (interface{}, error) {
	return fn(key, value)
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
	newKey, err := p.selectKey.SelectKey(msg.Key, msg.Value)
	if err != nil {
		return nil, err
	}
	return []commtypes.Message{{Key: newKey, Value: msg.Value, Timestamp: msg.Timestamp}}, nil
}
