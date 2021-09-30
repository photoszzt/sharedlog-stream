package processor

import "github.com/rs/zerolog/log"

type StreamReduceProcessor struct {
	pipe    Pipe
	store   KeyValueStore
	pctx    ProcessorContext
	reducer Reducer
}

var _ = Processor(&StreamReduceProcessor{})

func NewStreamReduceProcessor(reducer Reducer) *StreamReduceProcessor {
	return &StreamReduceProcessor{
		reducer: reducer,
	}
}

func (p *StreamReduceProcessor) WithPipe(pipe Pipe) {
	p.pipe = pipe
}

func (p *StreamReduceProcessor) WithProcessorContext(pctx ProcessorContext) {
	p.pctx = pctx
}

func (p *StreamReduceProcessor) Process(msg Message) error {
	newMsg, err := p.ProcessAndReturn(msg)
	if err != nil {
		return err
	}
	return p.pipe.Forward(*newMsg)
}

func (p *StreamReduceProcessor) ProcessAndReturn(msg Message) (*Message, error) {
	if msg.Key == nil || msg.Value == nil {
		log.Warn().Msgf("skipping record due to null key or value. key=%v, val=%v", msg.Key, msg.Value)
		return nil, nil
	}
	val, ok, err := p.store.Get(msg.Key)
	if err != nil {
		return nil, err
	}
	var newAgg interface{}
	var newTs uint64
	if ok {
		oldAggTs := val.(*ValueTimestamp)
		newAgg = p.reducer.Apply(oldAggTs.Value, msg.Value)
		if msg.Timestamp > oldAggTs.Timestamp {
			newTs = msg.Timestamp
		} else {
			newTs = oldAggTs.Timestamp
		}
	} else {
		newAgg = msg.Value
		newTs = msg.Timestamp
	}
	err = p.store.Put(msg.Key, &ValueTimestamp{Value: newAgg, Timestamp: newTs})
	if err != nil {
		return nil, err
	}
	return &Message{Key: msg.Key, Value: newAgg, Timestamp: newTs}, nil
}
