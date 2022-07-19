package processor

import (
	"context"
	"sharedlog-stream/pkg/commtypes"
)

type ProcessorChainIntr interface {
	Via(proc Processor[any, any, any, any]) ProcessorChainIntr
	Processors() []Processor[any, any, any, any]
	RunChains(ctx context.Context, initMsg commtypes.Message[any, any]) ([]commtypes.Message[any, any], error)
}

type ProcessorChains struct {
	procs []Processor[any, any, any, any]
}

func NewProcessorChains() ProcessorChains {
	return ProcessorChains{
		procs: make([]Processor[any, any, any, any], 0, 4),
	}
}

func (pc *ProcessorChains) Via(proc Processor[any, any, any, any]) ProcessorChainIntr {
	pc.procs = append(pc.procs, proc)
	return pc
}

func (pc *ProcessorChains) Processors() []Processor[any, any, any, any] {
	return pc.procs
}

func (pc *ProcessorChains) RunChains(ctx context.Context, initMsg commtypes.Message[any, any]) ([]commtypes.Message[any, any], error) {
	lastMsgs, err := pc.procs[0].ProcessAndReturn(ctx, initMsg)
	if err != nil {
		return nil, err
	}
	for i := 1; i < len(pc.procs); i++ {
		var outMsgs []commtypes.Message[any, any]
		for _, msg := range lastMsgs {
			out, err := pc.procs[i].ProcessAndReturn(ctx, msg)
			if err != nil {
				return nil, err
			}
			outMsgs = append(outMsgs, out...)
		}
		lastMsgs = outMsgs
	}
	return lastMsgs, nil
}

func ProcessMsg(ctx context.Context, initMsg commtypes.Message[any, any], argsTmp interface{}) error {
	args := argsTmp.(ExecutionContext)
	// the last processor is a output function
	_, err := args.RunChains(ctx, initMsg)
	return err
}
