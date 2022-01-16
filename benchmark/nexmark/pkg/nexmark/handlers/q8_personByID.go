package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/benchmark/common/benchutil"
	ntypes "sharedlog-stream/benchmark/nexmark/pkg/nexmark/types"
	"sharedlog-stream/benchmark/nexmark/pkg/nexmark/utils"
	"sharedlog-stream/pkg/hash"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/stream/processor"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"time"

	"cs.utexas.edu/zjia/faas/types"
	"golang.org/x/xerrors"
)

type q8PersonsByIDHandler struct {
	env   types.Environment
	cHash *hash.ConsistentHash
}

func NewQ8PersonsByID(env types.Environment) types.FuncHandler {
	return &q8PersonsByIDHandler{
		env:   env,
		cHash: hash.NewConsistentHash(),
	}
}

func (h *q8PersonsByIDHandler) Call(ctx context.Context, input []byte) ([]byte, error) {
	parsedInput := &common.QueryInput{}
	err := json.Unmarshal(input, parsedInput)
	if err != nil {
		return nil, err
	}
	output := h.Q8PersonsByID(ctx, parsedInput)
	encodedOutput, err := json.Marshal(output)
	if err != nil {
		panic(err)
	}
	return utils.CompressData(encodedOutput), nil
}

func (h *q8PersonsByIDHandler) process(
	ctx context.Context,
	argsTmp interface{},
	trackParFunc func([]uint8) error,
) (uint64, *common.FnOutput) {
	args := argsTmp.(*q8PersonsByIDProcessArgs)
	currentOffset := uint64(0)
	gotMsgs, err := args.src.Consume(ctx, args.parNum)
	if err != nil {
		if xerrors.Is(err, sharedlog_stream.ErrStreamSourceTimeout) {
			return currentOffset, &common.FnOutput{
				Success: true,
				Message: err.Error(),
			}
		}
		return currentOffset, &common.FnOutput{
			Success: false,
			Message: err.Error(),
		}
	}
	for _, msg := range gotMsgs {
		if msg.Msg.Value == nil {
			continue
		}
		currentOffset = msg.LogSeqNum
		event := msg.Msg.Value.(*ntypes.Event)
		ts, err := event.ExtractStreamTime()
		if err != nil {
			return currentOffset, &common.FnOutput{
				Success: false,
				Message: fmt.Sprintf("fail to extract timestamp: %v", err),
			}
		}
		msg.Msg.Timestamp = ts
		filteredMsgs, err := args.filterPerson.ProcessAndReturn(ctx, msg.Msg)
		if err != nil {
			return currentOffset, &common.FnOutput{
				Success: false,
				Message: fmt.Sprintf("filterPerson err: %v", err),
			}
		}
		for _, filteredMsg := range filteredMsgs {
			changeKeyedMsg, err := args.personsByIDMap.ProcessAndReturn(ctx, filteredMsg)
			if err != nil {
				return currentOffset, &common.FnOutput{
					Success: false,
					Message: fmt.Sprintf("personsByIDMap err: %v", err),
				}
			}

			k := changeKeyedMsg[0].Key.(uint64)
			parTmp, ok := h.cHash.Get(k)
			if !ok {
				return currentOffset, &common.FnOutput{
					Success: false,
					Message: "fail to get output partition",
				}
			}
			par := parTmp.(uint8)
			err = trackParFunc([]uint8{par})
			if err != nil {
				return currentOffset, &common.FnOutput{
					Success: false,
					Message: fmt.Sprintf("add topic partition failed: %v\n", err),
				}
			}
			err = args.sink.Sink(ctx, changeKeyedMsg[0], par, false)
			if err != nil {
				return currentOffset, &common.FnOutput{
					Success: false,
					Message: fmt.Sprintf("sink err: %v", err),
				}
			}
		}
	}
	return currentOffset, nil
}

type q8PersonsByIDProcessArgs struct {
	src           *processor.MeteredSource
	sink          *processor.MeteredSink
	output_stream *sharedlog_stream.ShardedSharedLogStream

	filterPerson   *processor.MeteredProcessor
	personsByIDMap *processor.MeteredProcessor
	parNum         uint8
}

func (h *q8PersonsByIDHandler) Q8PersonsByID(ctx context.Context, sp *common.QueryInput) *common.FnOutput {
	input_stream, output_stream, err := benchutil.GetShardedInputOutputStreams(ctx, h.env, sp)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: fmt.Sprintf("get input output stream failed: %v", err),
		}
	}
	msgSerde, err := commtypes.GetMsgSerde(sp.SerdeFormat)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: fmt.Sprintf("get msg serde err: %v", err),
		}
	}
	eventSerde, err := getEventSerde(sp.SerdeFormat)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: fmt.Sprintf("get event serde err: %v", err),
		}
	}
	src, sink, err := CommonGetSrcSink(ctx, sp, input_stream, output_stream, eventSerde, msgSerde)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: err.Error(),
		}
	}

	filterPerson := processor.NewMeteredProcessor(processor.NewStreamFilterProcessor(processor.PredicateFunc(
		func(msg *commtypes.Message) (bool, error) {
			event := msg.Value.(ntypes.Event)
			return event.Etype == ntypes.PERSON, nil
		})))
	personsByIDMap := processor.NewMeteredProcessor(processor.NewStreamMapProcessor(
		processor.MapperFunc(func(msg commtypes.Message) (commtypes.Message, error) {
			event := msg.Value.(*ntypes.Event)
			return commtypes.Message{
				Key:       event.NewPerson.ID,
				Value:     msg.Value,
				Timestamp: msg.Timestamp,
			}, nil
		})))

	procArgs := &q8PersonsByIDProcessArgs{
		src:            src,
		sink:           sink,
		output_stream:  output_stream,
		filterPerson:   filterPerson,
		personsByIDMap: personsByIDMap,
		parNum:         sp.ParNum,
	}

	task := sharedlog_stream.StreamTask{
		ProcessFunc: h.process,
	}

	for i := 0; i < int(sp.NumOutPartition); i++ {
		h.cHash.Add(uint8(i))
	}

	if sp.EnableTransaction {
		streamTaskArgs := sharedlog_stream.StreamTaskArgsTransaction{
			ProcArgs:        procArgs,
			Env:             h.env,
			Src:             src,
			OutputStream:    output_stream,
			QueryInput:      sp,
			TransactionalId: fmt.Sprintf("q3PersonsByID-%s-%d-%s", sp.InputTopicName, sp.ParNum, sp.OutputTopicName),
		}

		ret := task.ProcessWithTransaction(ctx, &streamTaskArgs)
		if ret != nil && ret.Success {
			ret.Latencies["src"] = src.GetLatency()
			ret.Latencies["sink"] = sink.GetLatency()
		}
		return ret
	}
	streamTaskArgs := sharedlog_stream.StreamTaskArgs{
		ProcArgs: procArgs,
		Duration: time.Duration(sp.Duration) * time.Second,
	}
	ret := task.Process(ctx, &streamTaskArgs)
	if ret != nil && ret.Success {
		ret.Latencies["src"] = src.GetLatency()
		ret.Latencies["sink"] = sink.GetLatency()
	}
	return ret
}
