package processor

import "github.com/rs/zerolog/log"

type StreamTableJoinProcessor struct {
	pipe      Pipe
	store     KeyValueStore
	pctx      ProcessorContext
	joiner    ValueJoinerWithKey
	leftJoin  bool
	storeName string
}

func NewStreamTableJoinProcessor(storeName string, joiner ValueJoinerWithKey) *StreamTableJoinProcessor {
	return &StreamTableJoinProcessor{
		storeName: storeName,
		joiner:    joiner,
	}
}

func (p *StreamTableJoinProcessor) WithPipe(pipe Pipe) {
	p.pipe = pipe
}

func (p *StreamTableJoinProcessor) WithProcessorContext(pctx ProcessorContext) {
	p.pctx = pctx
	p.store = p.pctx.GetKeyValueStore(p.storeName)
}

func (p *StreamTableJoinProcessor) Process(msg Message) error {
	if msg.Key == nil || msg.Value == nil {
		log.Warn().Msgf("Skipping record due to null join key or value. key=%v, val=%v", msg.Key, msg.Value)
		return nil
	}
	val2, ok, err := p.store.Get(msg.Key)
	if err != nil {
		return err
	}
	if p.leftJoin || ok {
		joined := p.joiner.Apply(msg.Key, msg.Value, val2)
		p.pipe.Forward(Message{Key: msg.Key, Value: joined, Timestamp: msg.Timestamp})
	}
	return nil
}
