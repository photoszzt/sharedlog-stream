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

var _ = Processor(&StreamTableJoinProcessor{})

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
	newMsg, err := p.ProcessAndReturn(msg)
	if err != nil {
		return err
	}
	if newMsg != nil {
		p.pipe.Forward(newMsg[0])
	}
	return nil
}

func (p *StreamTableJoinProcessor) ProcessAndReturn(msg Message) ([]Message, error) {
	if msg.Key == nil || msg.Value == nil {
		log.Warn().Msgf("Skipping record due to null join key or value. key=%v, val=%v", msg.Key, msg.Value)
		return nil, nil
	}
	val2, ok, err := p.store.Get(msg.Key)
	if err != nil {
		return nil, err
	}
	if p.leftJoin || ok {
		joined := p.joiner.Apply(msg.Key, msg.Value, val2)
		newMsg := Message{Key: msg.Key, Value: joined, Timestamp: msg.Timestamp}
		return []Message{newMsg}, nil
	}
	return nil, nil
}
