package processor

import (
	"context"
	"sharedlog-stream/pkg/commtypes"
)

type TableToStreamProcessor struct {
	valueMapperWithKey ValueMapperWithKey
	name               string
	BaseProcessor
}

var _ = Processor(&TableToStreamProcessor{})

func NewTableToStreamProcessor() *TableToStreamProcessor {
	p := &TableToStreamProcessor{
		name: "toStream",
		valueMapperWithKey: ValueMapperWithKeyFunc(func(key, value interface{}) (interface{}, error) {
			val := commtypes.CastToChangePtr(value)
			return val.NewVal, nil
		}),
	}
	p.BaseProcessor.ProcessingFunc = p.ProcessAndReturn
	return p
}

func (p *TableToStreamProcessor) Name() string {
	return p.name
}

func (p *TableToStreamProcessor) ProcessAndReturn(ctx context.Context, msg commtypes.Message) ([]commtypes.Message, error) {
	newV, err := p.valueMapperWithKey.MapValue(ctx, msg.Value)
	if err != nil {
		return nil, err
	}
	return []commtypes.Message{{Key: msg.Key, Value: newV, Timestamp: msg.Timestamp}}, nil
}
