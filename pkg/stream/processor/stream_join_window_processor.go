package processor

import (
	"context"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"sharedlog-stream/pkg/stream/processor/store"
)

type StreamJoinWindowProcessor struct {
	pipe       Pipe
	winStore   store.WindowStore
	pctx       store.ProcessorContext
	windowName string
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
func (p *StreamJoinWindowProcessor) Process(ctx context.Context, msg commtypes.Message) error {
	if msg.Key != nil {
		err := p.winStore.Put(ctx, msg.Key, msg.Value, msg.Timestamp)
		if err != nil {
			return err
		}
		return p.pipe.Forward(ctx, msg)
	}
	return nil
}

func (p *StreamJoinWindowProcessor) ProcessAndReturn(ctx context.Context, msg commtypes.Message) ([]commtypes.Message, error) {
	if msg.Key != nil {
		err := p.winStore.Put(ctx, msg.Key, msg.Value, msg.Timestamp)
		if err != nil {
			return nil, err
		}
		return []commtypes.Message{msg}, nil
	}
	return nil, nil
}
