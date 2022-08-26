package processor

import (
	"context"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/producer_consumer"

	"4d63.com/optional"
)

type FixedSubstreamOutputProcessor struct {
	producer producer_consumer.MeteredProducerIntr
	name     string
	BaseProcessor
	fixedSubstream uint8
}

var _ Processor = &FixedSubstreamOutputProcessor{}

func NewFixedSubstreamOutputProcessor(
	producer producer_consumer.MeteredProducerIntr, fixedSubNum uint8,
) Processor {
	r := &FixedSubstreamOutputProcessor{
		name:           "to" + producer.TopicName(),
		producer:       producer,
		fixedSubstream: fixedSubNum,
		BaseProcessor:  BaseProcessor{},
	}
	r.BaseProcessor.ProcessingFunc = r.ProcessAndReturn
	return r
}

func (p *FixedSubstreamOutputProcessor) Name() string {
	return p.name
}

func (p *FixedSubstreamOutputProcessor) ProcessAndReturn(ctx context.Context, msg commtypes.Message) ([]commtypes.Message, error) {
	err := p.producer.Produce(ctx, msg, p.fixedSubstream, false)
	return nil, err
}

type FixedSubstreamOutputProcessorG[KIn, VIn any] struct {
	producer producer_consumer.MeteredProducerIntr
	name     string
	BaseProcessorG[KIn, VIn, any, any]
	fixedSubstream uint8
}

var _ ProcessorG[int, int, any, any] = &FixedSubstreamOutputProcessorG[int, int]{}

func NewFixedSubstreamOutputProcessorG[KIn, VIn any](
	producer producer_consumer.MeteredProducerIntr, fixedSubNum uint8,
) *FixedSubstreamOutputProcessorG[KIn, VIn] {
	r := &FixedSubstreamOutputProcessorG[KIn, VIn]{
		name:           "to" + producer.TopicName(),
		producer:       producer,
		fixedSubstream: fixedSubNum,
		BaseProcessorG: BaseProcessorG[KIn, VIn, any, any]{},
	}
	r.BaseProcessorG.ProcessingFuncG = r.ProcessAndReturn
	return r
}

func (p *FixedSubstreamOutputProcessorG[KIn, VIn]) Name() string {
	return p.name
}

func (p *FixedSubstreamOutputProcessorG[KIn, VIn]) ProcessAndReturn(ctx context.Context,
	msg commtypes.MessageG[optional.Optional[KIn], optional.Optional[VIn]],
) ([]commtypes.MessageG[optional.Optional[any], optional.Optional[any]], error) {
	var k interface{}
	var v interface{}
	var ok bool
	k, ok = msg.Key.Get()
	if !ok {
		k = nil
	}
	v, ok = msg.Value.Get()
	if !ok {
		v = nil
	}
	err := p.producer.Produce(ctx, commtypes.Message{Key: k, Value: v, Timestamp: msg.Timestamp}, p.fixedSubstream, false)
	return nil, err
}
