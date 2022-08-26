package processor

import (
	"context"
	"sharedlog-stream/pkg/commtypes"

	"4d63.com/optional"
)

type Processor interface {
	Name() string
	// Process processes the stream commtypes.Message.
	ProcessAndReturn(context.Context, commtypes.Message) ([]commtypes.Message, error)
	IProcess
}

type CachedProcessor interface {
	Processor
	Flush(ctx context.Context) error
}

type IProcessG[KIn, VIn any] interface {
	Process(ctx context.Context, msg commtypes.MessageG[optional.Optional[KIn], optional.Optional[VIn]]) error
}

type IProcess interface {
	Process(ctx context.Context, msg commtypes.Message) error
}

type ProcessorG[KIn, VIn, KOut, VOut any] interface {
	Name() string
	ProcessAndReturn(ctx context.Context,
		msg commtypes.MessageG[optional.Optional[KIn], optional.Optional[VIn]],
	) ([]commtypes.MessageG[optional.Optional[KOut], optional.Optional[VOut]], error)
	IProcessG[KIn, VIn]
}

type CachedProcessorG[KIn, VIn, KOut, VOut any] interface {
	ProcessorG[KIn, VIn, KOut, VOut]
	Flush(ctx context.Context) error
}

type BaseProcessorG[KIn, VIn, KOut, VOut any] struct {
	ProcessingFuncG func(ctx context.Context, msg commtypes.MessageG[optional.Optional[KIn],
		optional.Optional[VIn]]) ([]commtypes.MessageG[optional.Optional[KOut], optional.Optional[VOut]], error)
	nextProcessors []IProcessG[KOut, VOut]
}

func (b *BaseProcessorG[KIn, VIn, KOut, VOut]) Process(ctx context.Context,
	msg commtypes.MessageG[optional.Optional[KIn], optional.Optional[VIn]],
) error {
	result, err := b.ProcessingFuncG(ctx, msg)
	if err != nil {
		return err
	}
	for _, nextProcessor := range b.nextProcessors {
		for _, nextMsg := range result {
			err = nextProcessor.Process(ctx, nextMsg)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

type BaseProcessor struct {
	ProcessingFunc func(ctx context.Context, msg commtypes.Message) ([]commtypes.Message, error)
	nextProcessors []IProcess
}

func (b *BaseProcessor) Process(ctx context.Context, msg commtypes.Message) error {
	result, err := b.ProcessingFunc(ctx, msg)
	if err != nil {
		return err
	}
	for _, nextProcessor := range b.nextProcessors {
		for _, nextMsg := range result {
			err = nextProcessor.Process(ctx, nextMsg)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
