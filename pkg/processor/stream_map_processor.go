package processor

import (
	"context"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/utils"

	"4d63.com/optional"
)

type Mapper interface {
	Map(key, value interface{}) (interface{} /* key */, interface{} /* value */, error)
}
type MapperFunc func(key, value interface{}) (interface{} /* key */, interface{} /* value */, error)

var _ = (Mapper)(MapperFunc(nil))

func (fn MapperFunc) Map(key, value interface{}) (interface{} /* key */, interface{} /* value */, error) {
	return fn(key, value)
}

type MapperG[K, V, KR, VR any] interface {
	Map(key K, value V) (KR /* key */, VR /* value */, error)
}
type MapperFuncG[K, V, KR, VR any] func(key K, value V) (KR /* key */, VR /* value */, error)

var _ = (MapperG[int, int, int, int])(MapperFuncG[int, int, int, int](nil))

func (fn MapperFuncG[K, V, KR, VR]) Map(key K, value V) (KR /* key */, VR /* value */, error) {
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

type StreamMapProcessorG[K, V, KR, VR any] struct {
	mapper MapperG[K, V, KR, VR]
	name   string
}

var _ = Processor(&StreamMapProcessorG[int, int, string, string]{})

func NewStreamMapProcessorG[K, V, KR, VR any](name string, mapper MapperG[K, V, KR, VR]) *StreamMapProcessorG[K, V, KR, VR] {
	return &StreamMapProcessorG[K, V, KR, VR]{
		mapper: mapper,
		name:   name,
	}
}
func (p *StreamMapProcessorG[K, V, KR, VR]) Name() string { return p.name }
func (p *StreamMapProcessorG[K, V, KR, VR]) ProcessAndReturn(ctx context.Context, msg commtypes.Message) ([]commtypes.Message, error) {
	newK, newV, err := p.mapper.Map(msg.Key.(K), msg.Value.(V))
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

type ValueMapperWithKeyG[K, V, VR any] interface {
	MapValue(key K, value V) (VR, error)
}
type ValueMapperWithKeyFuncG[K, V, VR any] func(key K, value V) (VR, error)

var _ = ValueMapperWithKeyG[int, int, string](ValueMapperWithKeyFuncG[int, int, string](nil))

func (fn ValueMapperWithKeyFuncG[K, V, VR]) MapValue(key K, value V) (VR, error) {
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

type StreamMapValuesProcessorG[K, V, VR any] struct {
	valueMapperWithKey ValueMapperWithKeyG[K, V, VR]
	name               string
}

var _ = Processor(&StreamMapValuesProcessorG[int, string, string]{})

func NewStreamMapValuesProcessorG[K, V, VR any](name string, mapper ValueMapperWithKeyG[K, V, VR]) *StreamMapValuesProcessorG[K, V, VR] {
	return &StreamMapValuesProcessorG[K, V, VR]{
		valueMapperWithKey: mapper,
		name:               name,
	}
}

func (p *StreamMapValuesProcessorG[K, V, VR]) Name() string {
	return p.name
}

func (p *StreamMapValuesProcessorG[K, V, VR]) ProcessAndReturn(ctx context.Context, msg commtypes.Message) ([]commtypes.Message, error) {
	newV, err := p.valueMapperWithKey.MapValue(msg.Key.(K), msg.Value.(V))
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

type SelectKeyMapperG[K, V, KR any] interface {
	SelectKey(key optional.Optional[K], value V) (KR, error)
}
type SelectKeyFuncG[K, V, KR any] func(key optional.Optional[K], value V) (KR, error)

var _ = SelectKeyMapperG[int, int, string](SelectKeyFuncG[int, int, string](nil))

func (fn SelectKeyFuncG[K, V, KR]) SelectKey(key optional.Optional[K], value V) (KR, error) {
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

type StreamSelectKeyProcessorG[K, V, KR any] struct {
	selectKey SelectKeyMapperG[K, V, KR]
	name      string
}

func NewStreamSelectKeyProcessorG[K, V, KR any](name string, keySelector SelectKeyMapperG[K, V, KR]) Processor {
	return &StreamSelectKeyProcessorG[K, V, KR]{
		name:      name,
		selectKey: keySelector,
	}
}

func (p *StreamSelectKeyProcessorG[K, V, KR]) Name() string {
	return p.name
}

func (p *StreamSelectKeyProcessorG[K, V, KR]) ProcessAndReturn(ctx context.Context, msg commtypes.Message) ([]commtypes.Message, error) {
	k := optional.Empty[K]()
	if !utils.IsNil(msg.Key) {
		k = optional.Of(msg.Key.(K))
	}
	newKey, err := p.selectKey.SelectKey(k, msg.Value.(V))
	if err != nil {
		return nil, err
	}
	return []commtypes.Message{{Key: newKey, Value: msg.Value, Timestamp: msg.Timestamp}}, nil
}
