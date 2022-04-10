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
	"sharedlog-stream/pkg/transaction"
	"sync"
	"time"

	"cs.utexas.edu/zjia/faas/types"
	"golang.org/x/xerrors"
)

type query3PersonsByIDHandler struct {
	env      types.Environment
	cHashMu  sync.RWMutex
	cHash    *hash.ConsistentHash
	funcName string
}

func NewQuery3PersonsByID(env types.Environment, funcName string) types.FuncHandler {
	return &query3PersonsByIDHandler{
		env:      env,
		cHash:    hash.NewConsistentHash(),
		funcName: funcName,
	}
}

func (h *query3PersonsByIDHandler) process(
	ctx context.Context,
	t *transaction.StreamTask,
	argsTmp interface{},
) (map[string]uint64, *common.FnOutput) {
	args := argsTmp.(*query3PersonsByIDProcessArgs)
	return transaction.CommonProcess(ctx, t, args, func(t *transaction.StreamTask, msg commtypes.MsgAndSeq) error {
		t.CurrentOffset[args.src.TopicName()] = msg.LogSeqNum
		if msg.MsgArr != nil {
			for _, subMsg := range msg.MsgArr {
				err := h.procMsg(ctx, subMsg, args)
				if err != nil {
					return err
				}
			}
			return nil
		}
		return h.procMsg(ctx, msg.Msg, args)
	})
}

func (h *query3PersonsByIDHandler) procMsg(ctx context.Context, msg commtypes.Message, args *query3PersonsByIDProcessArgs) error {
	event := msg.Value.(*ntypes.Event)
	ts, err := event.ExtractStreamTime()
	if err != nil {
		return fmt.Errorf("fail to extract timestamp: %v", err)
	}
	msg.Timestamp = ts
	filteredMsgs, err := args.filterPerson.ProcessAndReturn(ctx, msg)
	if err != nil {
		return fmt.Errorf("filterPerson err: %v", err)
	}
	for _, filteredMsg := range filteredMsgs {
		changeKeyedMsg, err := args.personsByIDMap.ProcessAndReturn(ctx, filteredMsg)
		if err != nil {
			return fmt.Errorf("personsByIDMap err: %v", err)
		}

		k := changeKeyedMsg[0].Key.(uint64)
		h.cHashMu.RLock()
		parTmp, ok := h.cHash.Get(k)
		h.cHashMu.RUnlock()
		if !ok {
			return xerrors.New("fail to get output partition")
		}
		par := parTmp.(uint8)
		err = args.trackParFunc(ctx, k, args.sink.KeySerde(), args.sink.TopicName(), par)
		if err != nil {
			return fmt.Errorf("add topic partition failed: %v", err)
		}
		// fmt.Fprintf(os.Stderr, "append %v to substream %v\n", changeKeyedMsg[0], par)
		err = args.sink.Sink(ctx, changeKeyedMsg[0], par, false)
		if err != nil {
			return fmt.Errorf("sink err: %v", err)
		}
	}
	return nil
}

func (h *query3PersonsByIDHandler) Call(ctx context.Context, input []byte) ([]byte, error) {
	parsedInput := &common.QueryInput{}
	err := json.Unmarshal(input, parsedInput)
	if err != nil {
		return nil, err
	}
	output := h.Query3PersonsByID(ctx, parsedInput)
	encodedOutput, err := json.Marshal(output)
	if err != nil {
		panic(err)
	}
	return utils.CompressData(encodedOutput), nil
}

type query3PersonsByIDProcessArgs struct {
	src              *processor.MeteredSource
	sink             *processor.MeteredSink
	output_stream    *sharedlog_stream.ShardedSharedLogStream
	filterPerson     *processor.MeteredProcessor
	personsByIDMap   *processor.MeteredProcessor
	trackParFunc     transaction.TrackKeySubStreamFunc
	recordFinishFunc transaction.RecordPrevInstanceFinishFunc
	funcName         string
	curEpoch         uint64
	parNum           uint8
}

func (a *query3PersonsByIDProcessArgs) Source() processor.Source { return a.src }
func (a *query3PersonsByIDProcessArgs) Sink() processor.Sink     { return a.sink }
func (a *query3PersonsByIDProcessArgs) ParNum() uint8            { return a.parNum }
func (a *query3PersonsByIDProcessArgs) CurEpoch() uint64         { return a.curEpoch }
func (a *query3PersonsByIDProcessArgs) FuncName() string         { return a.funcName }
func (a *query3PersonsByIDProcessArgs) RecordFinishFunc() func(ctx context.Context, funcName string, instanceId uint8) error {
	return a.recordFinishFunc
}

func (h *query3PersonsByIDHandler) Query3PersonsByID(ctx context.Context, sp *common.QueryInput) *common.FnOutput {
	input_stream, output_stream, err := benchutil.GetShardedInputOutputStreams(ctx, h.env, sp, false)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: fmt.Sprintf("get input output stream failed: %v", err),
		}
	}
	src, sink, msgSerde, err := CommonGetSrcSink(ctx, sp, input_stream, output_stream)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: err.Error(),
		}
	}

	filterPerson := processor.NewMeteredProcessor(processor.NewStreamFilterProcessor(processor.PredicateFunc(
		func(msg *commtypes.Message) (bool, error) {
			event := msg.Value.(*ntypes.Event)
			/*
				fmt.Fprintf(os.Stderr, "input event is %v\n", event)
				if event.Etype == ntypes.PERSON {
					fmt.Fprintf(os.Stderr, "person state is %v\n", event.NewPerson.State)
				}
			*/
			return event.Etype == ntypes.PERSON && ((event.NewPerson.State == "OR") ||
				event.NewPerson.State == "ID" || event.NewPerson.State == "CA"), nil
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

	procArgs := &query3PersonsByIDProcessArgs{
		src:              src,
		sink:             sink,
		output_stream:    output_stream,
		filterPerson:     filterPerson,
		personsByIDMap:   personsByIDMap,
		trackParFunc:     transaction.DefaultTrackSubstreamFunc,
		recordFinishFunc: transaction.DefaultRecordPrevInstanceFinishFunc,
		funcName:         h.funcName,
		curEpoch:         sp.ScaleEpoch,
		parNum:           sp.ParNum,
	}

	task := transaction.StreamTask{
		ProcessFunc:   h.process,
		CurrentOffset: make(map[string]uint64),
	}

	transaction.SetupConsistentHash(&h.cHashMu, h.cHash, sp.NumOutPartition)

	if sp.EnableTransaction {
		srcs := make(map[string]processor.Source)
		srcs[sp.InputTopicNames[0]] = src
		streamTaskArgs := transaction.StreamTaskArgsTransaction{
			ProcArgs:     procArgs,
			Env:          h.env,
			MsgSerde:     msgSerde,
			Srcs:         srcs,
			OutputStream: output_stream,
			QueryInput:   sp,
			TransactionalId: fmt.Sprintf("%s-%s-%d-%s",
				h.funcName, sp.InputTopicNames[0], sp.ParNum, sp.OutputTopicName),
			KVChangelogs:          nil,
			WindowStoreChangelogs: nil,
			FixedOutParNum:        0,
			CHash:                 h.cHash,
			CHashMu:               &h.cHashMu,
		}
		ret := transaction.SetupManagersAndProcessTransactional(ctx, h.env, &streamTaskArgs,
			func(procArgs interface{}, trackParFunc transaction.TrackKeySubStreamFunc,
				recordFinishFunc transaction.RecordPrevInstanceFinishFunc) {
				procArgs.(*query3PersonsByIDProcessArgs).trackParFunc = trackParFunc
				procArgs.(*query3PersonsByIDProcessArgs).recordFinishFunc = recordFinishFunc
			}, &task)
		if ret != nil && ret.Success {
			ret.Latencies["src"] = src.GetLatency()
			ret.Latencies["sink"] = sink.GetLatency()
			ret.Latencies["filterPerson"] = filterPerson.GetLatency()
			ret.Latencies["personsByIDMap"] = personsByIDMap.GetLatency()
			ret.Consumed["src"] = src.GetCount()
		}
		return ret
	}
	streamTaskArgs := transaction.StreamTaskArgs{
		ProcArgs: procArgs,
		Duration: time.Duration(sp.Duration) * time.Second,
	}
	ret := task.Process(ctx, &streamTaskArgs)
	if ret != nil && ret.Success {
		ret.Latencies["src"] = src.GetLatency()
		ret.Latencies["sink"] = sink.GetLatency()
		ret.Latencies["filterPerson"] = filterPerson.GetLatency()
		ret.Latencies["personsByIDMap"] = personsByIDMap.GetLatency()
		ret.Consumed["src"] = src.GetCount()
	}
	return ret
}
