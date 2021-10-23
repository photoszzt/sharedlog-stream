package processor

import (
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"sharedlog-stream/pkg/stream/processor/store"
)

type StreamJoinWindowProcessor struct {
	pipe       Pipe
	winStore   store.WindowStore
	pctx       store.ProcessorContext
	windowName string
	latencies  []int
}

var _ = Processor(&StreamJoinWindowProcessor{})

func NewStreamJoinWindowProcessor(windowName string) *StreamJoinWindowProcessor {
	return &StreamJoinWindowProcessor{
		windowName: windowName,
	}
}

// WithPipe sets the pipe on the Processor.
func (p *StreamJoinWindowProcessor) WithPipe(pipe Pipe) {
	p.pipe = pipe
}

// WithProcessorContext sets the context on the processor
func (p *StreamJoinWindowProcessor) WithProcessorContext(pctx store.ProcessorContext) {
	p.pctx = pctx
	p.winStore = p.pctx.GetWindowStore(p.windowName)
}

// Process processes the stream commtypes.Message.
func (p *StreamJoinWindowProcessor) Process(msg commtypes.Message) error {
	if msg.Key != nil {
		p.winStore.Put(msg.Key, msg.Value, msg.Timestamp)
		p.pipe.Forward(msg)
	}
	return nil
}

func (p *StreamJoinWindowProcessor) ProcessAndReturn(msg commtypes.Message) ([]commtypes.Message, error) {
	if msg.Key != nil {
		err := p.winStore.Put(msg.Key, msg.Value, msg.Timestamp)
		if err != nil {
			return nil, err
		}
		return []commtypes.Message{msg}, nil
	}
	return nil, nil
}
