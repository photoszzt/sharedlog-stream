package processor

import (
	"context"
	"fmt"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/producer_consumer"
)

type RoundRobinOutputProcessorG[KIn, VIn any] struct {
	producer   producer_consumer.MeteredProducerIntr
	ectx       ExecutionContext
	msgGSerdeG commtypes.MessageGSerdeG[KIn, VIn]
	name       string
	BaseProcessorG[KIn, VIn, any, any]
	curSubstream uint8
}

var _ ProcessorG[int, int, any, any] = &RoundRobinOutputProcessorG[int, int]{}

func NewRoundRobinOutputProcessorG[KIn, VIn any](processTimeTag string,
	ectx ExecutionContext,
	producer producer_consumer.MeteredProducerIntr,
	msgGSerdeG commtypes.MessageGSerdeG[KIn, VIn],
) *RoundRobinOutputProcessorG[KIn, VIn] {
	r := &RoundRobinOutputProcessorG[KIn, VIn]{
		producer:       producer,
		ectx:           ectx,
		msgGSerdeG:     msgGSerdeG,
		name:           "to" + producer.TopicName(),
		BaseProcessorG: BaseProcessorG[KIn, VIn, any, any]{},
		curSubstream:   0,
	}
	r.BaseProcessorG.ProcessingFuncG = r.ProcessAndReturn
	return r
}

func (p *RoundRobinOutputProcessorG[KIn, VIn]) OutputRemainingStats() {
	// p.procTimeStats.PrintRemainingStats()
}

func (p *RoundRobinOutputProcessorG[KIn, VIn]) Name() string {
	return p.name
}

func (p *RoundRobinOutputProcessorG[KIn, VIn]) ProcessAndReturn(ctx context.Context,
	msg commtypes.MessageG[KIn, VIn],
) ([]commtypes.MessageG[any, any], error) {
	msgSerOp, err := commtypes.MsgGToMsgSer(msg, p.msgGSerdeG.GetKeySerdeG(), p.msgGSerdeG.GetValSerdeG())
	if err != nil {
		return nil, err
	}
	msgSer, ok := msgSerOp.Take()
	if ok {
		par := p.curSubstream
		p.curSubstream = (p.curSubstream + 1) % p.producer.Stream().NumPartition()
		err = p.ectx.TrackParFunc()(p.producer.TopicName(), par)
		if err != nil {
			return nil, fmt.Errorf("track substream failed: %v", err)
		}
		err = p.producer.ProduceData(ctx, msgSer, par)
		return nil, err
	} else {
		return nil, nil
	}
}
