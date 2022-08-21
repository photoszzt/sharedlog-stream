package processor

import (
	"context"
	"fmt"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/hashfuncs"
	"sharedlog-stream/pkg/producer_consumer"
)

type GroupByOutputProcessor struct {
	byteSliceHasher hashfuncs.ByteSliceHasher
	producer        producer_consumer.MeteredProducerIntr
	ectx            ExecutionContext
	// cHash    *hash.ConsistentHash
	name string
}

func NewGroupByOutputProcessor(producer producer_consumer.MeteredProducerIntr,
	ectx ExecutionContext) Processor {
	// numPartition := producer.Stream().NumPartition()
	g := GroupByOutputProcessor{
		// cHash:    hash.NewConsistentHash(),
		producer:        producer,
		name:            "to" + producer.TopicName(),
		ectx:            ectx,
		byteSliceHasher: hashfuncs.ByteSliceHasher{},
	}
	// for i := uint8(0); i < numPartition; i++ {
	// 	g.cHash.Add(i)
	// }
	return &g
}

func (g *GroupByOutputProcessor) Name() string {
	return g.name
}

func (g *GroupByOutputProcessor) ProcessAndReturn(ctx context.Context, msg commtypes.Message,
) ([]commtypes.Message, error) {
	// parTmp, ok := g.cHash.Get(msg.Key)
	// if !ok {
	// 	return nil, common_errors.ErrFailToGetOutputSubstream
	// }
	kBytes, err := g.producer.KeyEncoder().Encode(msg.Key)
	if err != nil {
		return nil, err
	}
	hash := g.byteSliceHasher.HashSum64(kBytes)
	par := uint8(hash % uint64(g.producer.Stream().NumPartition()))
	err = g.ectx.TrackParFunc()(ctx, kBytes, g.producer.TopicName(), par)
	if err != nil {
		return nil, fmt.Errorf("track substream failed: %v", err)
	}
	err = g.producer.Produce(ctx, msg, par, false)
	return nil, err
}
