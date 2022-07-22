package processor

import (
	"context"
	"fmt"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/hashfuncs"
	"sharedlog-stream/pkg/producer_consumer"
	"sharedlog-stream/pkg/utils"
)

type GroupByOutputProcessor struct {
	producer producer_consumer.MeteredProducerIntr
	ectx     ExecutionContext
	// cHash    *hash.ConsistentHash
	name string
}

func NewGroupByOutputProcessor(producer producer_consumer.MeteredProducerIntr,
	ectx ExecutionContext) Processor {
	// numPartition := producer.Stream().NumPartition()
	g := GroupByOutputProcessor{
		// cHash:    hash.NewConsistentHash(),
		producer: producer,
		name:     "to" + producer.TopicName(),
		ectx:     ectx,
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

	hash := hashfuncs.NameHash(utils.GetStringValue(msg.Key))
	par := uint8(hash % uint64(g.producer.Stream().NumPartition()))
	err := g.ectx.TrackParFunc()(ctx, msg.Key, g.producer.KeySerde(), g.producer.TopicName(), par)
	if err != nil {
		return nil, fmt.Errorf("track substream failed: %v", err)
	}
	err = g.producer.Produce(ctx, msg, par, false)
	return nil, err
}
