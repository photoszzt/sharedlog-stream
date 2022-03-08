package handlers

import (
	"context"
	ntypes "sharedlog-stream/benchmark/nexmark/pkg/nexmark/types"
	"sharedlog-stream/pkg/stream/processor"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"sharedlog-stream/pkg/stream/processor/store"
	"time"
)

type Q7TransformProcessor struct {
	pipe        processor.Pipe
	windowStore store.WindowStore
	pctx        store.StoreContext
}

var _ = processor.Processor(&Q7TransformProcessor{})

func NewQ7TransformProcessor(windowStore store.WindowStore) *Q7TransformProcessor {
	return &Q7TransformProcessor{
		windowStore: windowStore,
	}
}

func (p *Q7TransformProcessor) WithPipe(pipe processor.Pipe) {
	p.pipe = pipe
}

func (p *Q7TransformProcessor) WithProcessorContext(pctx store.StoreContext) {
	p.pctx = pctx
}

func (p *Q7TransformProcessor) Process(ctx context.Context, msg commtypes.Message) error {
	rets, err := p.ProcessAndReturn(ctx, msg)
	if err != nil {
		return err
	}
	for _, ret := range rets {
		err := p.pipe.Forward(ctx, ret)
		if err != nil {
			return err
		}
	}
	return nil
}

func (p *Q7TransformProcessor) ProcessAndReturn(ctx context.Context, msg commtypes.Message) ([]commtypes.Message, error) {
	key := msg.Key.(uint64)
	event := msg.Value.(*ntypes.Event)
	result := make([]commtypes.Message, 0, 128)
	err := p.windowStore.Fetch(ctx, key, time.UnixMilli((event.Bid.DateTime - 10*1000)),
		time.UnixMilli(event.Bid.DateTime),
		func(u int64, kt commtypes.KeyT, vt commtypes.ValueT) error {
			val := vt.(*commtypes.ValueTimestamp).Value.(*ntypes.PriceTime)
			if event.Bid.Price == val.Price {
				result = append(result, commtypes.Message{
					Key: key,
					Value: &ntypes.BidAndMax{
						Price:       event.Bid.Price,
						Auction:     event.Bid.Auction,
						Bidder:      event.Bid.Bidder,
						DateTime:    event.Bid.DateTime,
						Extra:       event.Bid.Extra,
						MaxDateTime: val.DateTime,
					},
					Timestamp: event.Bid.DateTime,
				})
			}
			return nil
		})
	if err != nil {
		return nil, err
	}
	return result, nil
}
