package processor

import (
	"context"
	"os"
	"sharedlog-stream/pkg/debug"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"sharedlog-stream/pkg/stream/processor/store"

	"github.com/rs/zerolog/log"
)

type TableTableJoinProcessor struct {
	pipe              Pipe
	store             store.KeyValueStore
	pctx              store.StoreContext
	joiner            ValueJoinerWithKey
	streamTimeTracker commtypes.StreamTimeTracker
	storeName         string
}

var _ = Processor(&StreamTableJoinProcessor{})

func NewTableTableJoinProcessor(storeName string, store store.KeyValueStore, joiner ValueJoinerWithKey) *TableTableJoinProcessor {
	return &TableTableJoinProcessor{
		storeName:         storeName,
		joiner:            joiner,
		store:             store,
		streamTimeTracker: commtypes.NewStreamTimeTracker(),
	}
}

func (p *TableTableJoinProcessor) WithPipe(pipe Pipe) {
	p.pipe = pipe
}

func (p *TableTableJoinProcessor) WithProcessorContext(pctx store.StoreContext) {
	p.pctx = pctx
	p.store = p.pctx.GetKeyValueStore(p.storeName)
}

func (p *TableTableJoinProcessor) Process(ctx context.Context, msg commtypes.Message) error {
	newMsg, err := p.ProcessAndReturn(ctx, msg)
	if err != nil {
		return err
	}
	if newMsg != nil {
		err := p.pipe.Forward(ctx, newMsg[0])
		if err != nil {
			return err
		}
	}
	return nil
}

func (p *TableTableJoinProcessor) ProcessAndReturn(ctx context.Context, msg commtypes.Message) ([]commtypes.Message, error) {
	if msg.Key == nil {
		log.Warn().Msgf("Skipping record due to null join key. key=%v", msg.Key)
		return nil, nil
	}
	val2, ok, err := p.store.Get(ctx, msg.Key)
	if err != nil {
		return nil, err
	}
	if ok {
		rv := val2.(commtypes.ValueTimestamp)
		if rv.Value == nil {
			return nil, nil
		}
		p.streamTimeTracker.UpdateStreamTime(&msg)
		ts := p.streamTimeTracker.GetStreamTime()
		debug.Fprintf(os.Stderr, "stream time: %d\n", ts)
		if ts < rv.Timestamp {
			ts = rv.Timestamp
		}
		if msg.Value != nil {
			joined := p.joiner.Apply(msg.Key, msg.Value, val2)
			newMsg := commtypes.Message{Key: msg.Key, Value: joined, Timestamp: ts}
			return []commtypes.Message{newMsg}, nil
		} else {
			return []commtypes.Message{{Key: msg.Key, Value: nil, Timestamp: ts}}, nil
		}
	}
	return nil, nil
}
