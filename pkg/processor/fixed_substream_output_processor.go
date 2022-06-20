package processor

import (
	"context"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/producer_consumer"
)

type FixedSubstreamOutputProcessor struct {
	producer       producer_consumer.MeteredProducerIntr
	name           string
	fixedSubstream uint8
}

func NewFixedSubstreamOutputProcessor(
	producer producer_consumer.MeteredProducerIntr, fixedSubNum uint8,
) Processor {
	return &FixedSubstreamOutputProcessor{
		name:           "to" + producer.TopicName(),
		producer:       producer,
		fixedSubstream: fixedSubNum,
	}
}

func (p *FixedSubstreamOutputProcessor) Name() string {
	return p.name
}

func (p *FixedSubstreamOutputProcessor) Process(ctx context.Context, msg commtypes.Message) error {
	panic("not implemented")
}

func (p *FixedSubstreamOutputProcessor) ProcessAndReturn(ctx context.Context, msg commtypes.Message) ([]commtypes.Message, error) {
	err := p.producer.Produce(ctx, msg, p.fixedSubstream, false)
	return nil, err
}
