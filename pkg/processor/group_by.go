package processor

import (
	"context"
	"fmt"
	"sharedlog-stream/pkg/common_errors"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/hash"
	"sharedlog-stream/pkg/source_sink"
	"sharedlog-stream/pkg/transaction/tran_interface"
)

type GroupBy struct {
	cHash *hash.ConsistentHash
	sink  source_sink.Sink
}

func NewGroupBy(sink source_sink.Sink) *GroupBy {
	numPartition := sink.Stream().NumPartition()
	g := GroupBy{
		cHash: hash.NewConsistentHash(),
		sink:  sink,
	}
	for i := uint8(0); i < numPartition; i++ {
		g.cHash.Add(i)
	}
	return &g
}

func (g *GroupBy) GroupByAndProduce(ctx context.Context, msg commtypes.Message,
	trackParFunc tran_interface.TrackKeySubStreamFunc,
) error {
	parTmp, ok := g.cHash.Get(msg.Key)
	if !ok {
		return common_errors.ErrFailToGetOutputSubstream
	}
	par := parTmp.(uint8)
	err := trackParFunc(ctx, msg.Key, g.sink.KeySerde(), g.sink.TopicName(), par)
	if err != nil {
		return fmt.Errorf("track substream failed: %v", err)
	}
	err = g.sink.Produce(ctx, msg, par, false)
	if err != nil {
		return fmt.Errorf("sink err: %v", err)
	}
	return nil
}
