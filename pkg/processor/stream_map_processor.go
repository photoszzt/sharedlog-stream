package processor

import (
	"context"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/store"
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
	pctx   store.StoreContext
	name   string
}

var _ = Processor(&StreamMapProcessor{})

func NewStreamMapProcessor(name string, mapper Mapper) *StreamMapProcessor {
	return &StreamMapProcessor{
		mapper: mapper,
		name:   name,
	}
}

func (p *StreamMapProcessor) WithPipe(pipe Pipe) {
	p.pipe = pipe
}

func (p *StreamMapProcessor) WithProcessorContext(pctx store.StoreContext) {
	p.pctx = pctx
}

func (p *StreamMapProcessor) Name() string {
	return p.name
}

func (p *StreamMapProcessor) Process(ctx context.Context, msg commtypes.Message) error {
	m, err := p.mapper.Map(msg)
	if err != nil {
		return err
	}
	m.Timestamp = msg.Timestamp
	return p.pipe.Forward(ctx, m)
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
	pipe        Pipe
	valueMapper ValueMapper
	pctx        store.StoreContext
	name        string
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

func (p *StreamMapValuesProcessor) WithProcessorContext(pctx store.StoreContext) {
	p.pctx = pctx
}

func (p *StreamMapValuesProcessor) Name() string {
	return p.name
}

func (p *StreamMapValuesProcessor) Process(ctx context.Context, msg commtypes.Message) error {
	newV, err := p.valueMapper.MapValue(msg.Value)
	if err != nil {
		return err
	}
	return p.pipe.Forward(ctx, commtypes.Message{Key: msg.Key, Value: newV, Timestamp: msg.Timestamp})
}

func (p *StreamMapValuesProcessor) ProcessAndReturn(ctx context.Context, msg commtypes.Message) ([]commtypes.Message, error) {
	newV, err := p.valueMapper.MapValue(msg.Value)
	if err != nil {
		return nil, err
	}
	return []commtypes.Message{{Key: msg.Key, Value: newV, Timestamp: msg.Timestamp}}, nil
}

type StreamMapValuesWithKeyProcessor struct {
	pipe               Pipe
	valueWithKeyMapper Mapper
	pctx               store.StoreContext
	name               string
}

func NewStreamMapValuesWithKeyProcessor(mapper Mapper) Processor {
	return &StreamMapValuesWithKeyProcessor{
		valueWithKeyMapper: mapper,
	}
}

func (p *StreamMapValuesWithKeyProcessor) WithPipe(pipe Pipe) {
	p.pipe = pipe
}

func (p *StreamMapValuesWithKeyProcessor) WithProcessorContext(pctx store.StoreContext) {
	p.pctx = pctx
}

func (p *StreamMapValuesWithKeyProcessor) Name() string {
	return p.name
}

func (p *StreamMapValuesWithKeyProcessor) Process(ctx context.Context, msg commtypes.Message) error {
	newMsg, err := p.ProcessAndReturn(ctx, msg)
	if err != nil {
		return err
	}
	return p.pipe.Forward(ctx, newMsg[0])
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
	pipe      Pipe
	selectKey SelectKeyMapper
	pctx      store.StoreContext
	name      string
}

func NewStreamSelectKeyProcessor(name string, keySelector SelectKeyMapper) Processor {
	return &StreamSelectKeyProcessor{
		name:      name,
		selectKey: keySelector,
	}
}

func (p *StreamSelectKeyProcessor) WithPipe(pipe Pipe) {
	p.pipe = pipe
}

func (p *StreamSelectKeyProcessor) WithProcessorContext(pctx store.StoreContext) {
	p.pctx = pctx
}
func (p *StreamSelectKeyProcessor) Name() string {
	return p.name
}

func (p *StreamSelectKeyProcessor) Process(ctx context.Context, msg commtypes.Message) error {
	newMsg, err := p.ProcessAndReturn(ctx, msg)
	if err != nil {
		return err
	}
	return p.pipe.Forward(ctx, newMsg[0])
}

func (p *StreamSelectKeyProcessor) ProcessAndReturn(ctx context.Context, msg commtypes.Message) ([]commtypes.Message, error) {
	newKey, err := p.selectKey.SelectKey(msg)
	if err != nil {
		return nil, err
	}
	return []commtypes.Message{{Key: newKey, Value: msg.Value, Timestamp: msg.Timestamp}}, nil
}
