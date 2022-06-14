package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/benchmark/common/benchutil"
	ntypes "sharedlog-stream/benchmark/nexmark/pkg/nexmark/types"
	"sharedlog-stream/benchmark/nexmark/pkg/nexmark/utils"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/execution"
	"sharedlog-stream/pkg/proc_interface"
	"sharedlog-stream/pkg/processor"
	"sharedlog-stream/pkg/source_sink"
	"sharedlog-stream/pkg/stream_task"
	"sync"
	"time"

	"cs.utexas.edu/zjia/faas/types"
)

type q8GroupByHandler struct {
	env      types.Environment
	funcName string
}

func NewQ8GroupByHandler(env types.Environment, funcName string) types.FuncHandler {
	return &q8GroupByHandler{
		env:      env,
		funcName: funcName,
	}
}

func (h *q8GroupByHandler) procMsg(ctx context.Context, msg commtypes.Message, argsTmp interface{}) error {
	args := argsTmp.(*TwoMsgChanProcArgs)
	args.msgChan1 <- msg
	args.msgChan2 <- msg
	return nil
}

func (h *q8GroupByHandler) Call(ctx context.Context, input []byte) ([]byte, error) {
	parsedInput := &common.QueryInput{}
	err := json.Unmarshal(input, parsedInput)
	if err != nil {
		return nil, err
	}
	output := h.Q8GroupBy(ctx, parsedInput)
	encodedOutput, err := json.Marshal(output)
	if err != nil {
		panic(err)
	}
	return utils.CompressData(encodedOutput), nil
}

func (h *q8GroupByHandler) getSrcSink(ctx context.Context, sp *common.QueryInput,
) ([]source_sink.MeteredSourceIntr, []source_sink.MeteredSink, error) {
	var sinks []source_sink.MeteredSink
	input_stream, output_streams, err := benchutil.GetShardedInputOutputStreams(ctx, h.env, sp)
	if err != nil {
		return nil, nil, err
	}
	serdeFormat := commtypes.SerdeFormat(sp.SerdeFormat)
	msgSerde, err := commtypes.GetMsgSerde(serdeFormat)
	if err != nil {
		return nil, nil, fmt.Errorf("get msg serde err: %v", err)
	}
	eventSerde, err := ntypes.GetEventSerde(serdeFormat)
	if err != nil {
		return nil, nil, fmt.Errorf("get event serde err: %v", err)
	}
	inConfig := &source_sink.StreamSourceConfig{
		Timeout: common.SrcConsumeTimeout,
		KVMsgSerdes: commtypes.KVMsgSerdes{
			KeySerde: commtypes.StringSerde{},
			ValSerde: eventSerde,
			MsgSerde: msgSerde,
		},
	}
	outConfig := &source_sink.StreamSinkConfig{
		KVMsgSerdes: commtypes.KVMsgSerdes{
			KeySerde: commtypes.Uint64Serde{},
			ValSerde: eventSerde,
			MsgSerde: msgSerde,
		},
		FlushDuration: time.Duration(sp.FlushMs) * time.Millisecond,
	}
	// fmt.Fprintf(os.Stderr, "output to %v\n", output_stream.TopicName())
	src := source_sink.NewMeteredSource(source_sink.NewShardedSharedLogStreamSource(input_stream, inConfig), time.Duration(sp.WarmupS)*time.Second)
	src.SetInitialSource(true)
	for _, output_stream := range output_streams {
		sink := source_sink.NewMeteredSyncSink(source_sink.NewShardedSharedLogStreamSyncSink(output_stream, outConfig), time.Duration(sp.WarmupS)*time.Second)
		sinks = append(sinks, sink)
	}
	return []source_sink.MeteredSourceIntr{src}, sinks, nil
}

func (h *q8GroupByHandler) Q8GroupBy(ctx context.Context, sp *common.QueryInput) *common.FnOutput {
	srcs, sinks_arr, err := h.getSrcSink(ctx, sp)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	filterPerson, personsByIDMap, personsByIDFunc := h.getPersonsByID(time.Duration(sp.WarmupS)*time.Second,
		time.Duration(sp.FlushMs)*time.Millisecond)
	filterAuctions, auctionsBySellerIDMap, auctionsBySellerIDFunc := h.getAucBySellerID(time.Duration(sp.WarmupS)*time.Second,
		time.Duration(sp.FlushMs)*time.Millisecond)

	var wg sync.WaitGroup
	personsByIDManager := execution.NewGeneralProcManager(personsByIDFunc)
	auctionsBySellerIDManager := execution.NewGeneralProcManager(auctionsBySellerIDFunc)
	procArgs := &TwoMsgChanProcArgs{
		msgChan1:             auctionsBySellerIDManager.MsgChan(),
		msgChan2:             personsByIDManager.MsgChan(),
		BaseExecutionContext: proc_interface.NewExecutionContext(srcs, sinks_arr, h.funcName, sp.ScaleEpoch, sp.ParNum),
	}

	personsByIDManager.LaunchProc(ctx, procArgs, &wg)
	auctionsBySellerIDManager.LaunchProc(ctx, procArgs, &wg)

	handleErrFunc := func() error {
		select {
		case perErr := <-personsByIDManager.ErrChan():
			return perErr
		case aucErr := <-auctionsBySellerIDManager.ErrChan():
			return aucErr
		default:
		}
		return nil
	}

	task := stream_task.NewStreamTaskBuilder().
		AppProcessFunc(func(ctx context.Context, task *stream_task.StreamTask, argsTmp interface{}) *common.FnOutput {
			args := argsTmp.(proc_interface.ExecutionContext)
			return execution.CommonProcess(ctx, task, args, h.procMsg)
		}).
		InitFunc(func(progArgs interface{}) {
			filterPerson.StartWarmup()
			personsByIDMap.StartWarmup()
			filterAuctions.StartWarmup()
			auctionsBySellerIDMap.StartWarmup()
		}).
		PauseFunc(func() *common.FnOutput {
			// debug.Fprintf(os.Stderr, "begin flush\n")
			personsByIDManager.RequestToTerminate()
			auctionsBySellerIDManager.RequestToTerminate()
			wg.Wait()
			if err := handleErrFunc(); err != nil {
				return &common.FnOutput{Success: false, Message: err.Error()}
			}
			// debug.Fprintf(os.Stderr, "done flush\n")
			return nil
		}).
		ResumeFunc(func(task *stream_task.StreamTask) {
			// debug.Fprintf(os.Stderr, "begin resume\n")
			personsByIDManager.RecreateMsgChan(&procArgs.msgChan2)
			auctionsBySellerIDManager.RecreateMsgChan(&procArgs.msgChan1)
			personsByIDManager.LaunchProc(ctx, procArgs, &wg)
			auctionsBySellerIDManager.LaunchProc(ctx, procArgs, &wg)
			// debug.Fprintf(os.Stderr, "done resume\n")
		}).
		HandleErrFunc(handleErrFunc).Build()

	update_stats := func(ret *common.FnOutput) {
		ret.Latencies["filterPerson"] = filterPerson.GetLatency()
		ret.Latencies["personsByIDMap"] = personsByIDMap.GetLatency()
		ret.Latencies["filterAuctions"] = filterAuctions.GetLatency()
		ret.Latencies["auctionsBySellerIDMap"] = auctionsBySellerIDMap.GetLatency()
	}
	transactionalID := fmt.Sprintf("%s-%s-%d",
		h.funcName, sp.InputTopicNames[0], sp.ParNum)
	streamTaskArgs := benchutil.UpdateStreamTaskArgs(sp,
		stream_task.NewStreamTaskArgsBuilder(h.env, procArgs, transactionalID)).
		Build()
	return task.ExecuteApp(ctx, streamTaskArgs, sp.EnableTransaction, update_stats)
}

func (h *q8GroupByHandler) getPersonsByID(warmup time.Duration, pollTimeout time.Duration) (
	*processor.MeteredProcessor,
	*processor.MeteredProcessor,
	execution.GeneralProcFunc,
) {
	filterPerson := processor.NewMeteredProcessor(processor.NewStreamFilterProcessor(processor.PredicateFunc(
		func(msg *commtypes.Message) (bool, error) {
			event := msg.Value.(*ntypes.Event)
			return event.Etype == ntypes.PERSON, nil
		})), warmup)
	personsByIDMap := processor.NewMeteredProcessor(processor.NewStreamMapProcessor(
		processor.MapperFunc(func(msg commtypes.Message) (commtypes.Message, error) {
			event := msg.Value.(*ntypes.Event)
			return commtypes.Message{
				Key:       event.NewPerson.ID,
				Value:     msg.Value,
				Timestamp: msg.Timestamp,
			}, nil
		})), warmup)

	return filterPerson, personsByIDMap,
		func(ctx context.Context, argsTmp interface{}, wg *sync.WaitGroup,
			msgChan chan commtypes.Message, errChan chan error,
		) {
			args := argsTmp.(*TwoMsgChanProcArgs)
			g := processor.NewGroupBy(args.Sinks()[1])
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case msg, ok := <-msgChan:
					if !ok {
						return
					}
					filteredMsgs, err := filterPerson.ProcessAndReturn(ctx, msg)
					if err != nil {
						fmt.Fprintf(os.Stderr, "[ERROR] filterPerson err: %v\n", err)
						errChan <- fmt.Errorf("filterPerson err: %v", err)
						return
					}
					for _, filteredMsg := range filteredMsgs {
						changeKeyedMsg, err := personsByIDMap.ProcessAndReturn(ctx, filteredMsg)
						if err != nil {
							fmt.Fprintf(os.Stderr, "[ERROR] personsByIDMap err: %v\n", err)
							errChan <- fmt.Errorf("personsByIDMap err: %v", err)
							return
						}
						err = g.GroupByAndProduce(ctx, changeKeyedMsg[0], args.TrackParFunc())
						if err != nil {
							fmt.Fprintf(os.Stderr, "[ERROR] groupby failed: %v", err)
							errChan <- err
							return
						}
					}
				}
			}
		}
}

func (h *q8GroupByHandler) getAucBySellerID(warmup time.Duration, pollTimeout time.Duration) (*processor.MeteredProcessor, *processor.MeteredProcessor,
	execution.GeneralProcFunc,
) {
	filterAuctions := processor.NewMeteredProcessor(processor.NewStreamFilterProcessor(processor.PredicateFunc(
		func(m *commtypes.Message) (bool, error) {
			event := m.Value.(*ntypes.Event)
			return event.Etype == ntypes.AUCTION, nil
		})), warmup)

	auctionsBySellerIDMap := processor.NewMeteredProcessor(processor.NewStreamMapProcessor(
		processor.MapperFunc(func(msg commtypes.Message) (commtypes.Message, error) {
			event := msg.Value.(*ntypes.Event)
			return commtypes.Message{Key: event.NewAuction.Seller, Value: msg.Value, Timestamp: msg.Timestamp}, nil
		})), warmup)

	return filterAuctions, auctionsBySellerIDMap, func(ctx context.Context, argsTmp interface{}, wg *sync.WaitGroup, msgChan chan commtypes.Message, errChan chan error) {
		args := argsTmp.(*TwoMsgChanProcArgs)
		g := processor.NewGroupBy(args.Sinks()[0])
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-msgChan:
				if !ok {
					return
				}
				filteredMsgs, err := filterAuctions.ProcessAndReturn(ctx, msg)
				if err != nil {
					fmt.Fprintf(os.Stderr, "[ERROR] filterAuctions err: %v\n", err)
					errChan <- fmt.Errorf("filterAuctions err: %v", err)
					return
				}
				for _, filteredMsg := range filteredMsgs {
					changeKeyedMsg, err := auctionsBySellerIDMap.ProcessAndReturn(ctx, filteredMsg)
					if err != nil {
						fmt.Fprintf(os.Stderr, "[ERROR] auctionsBySellerIDMap err: %v\n", err)
						errChan <- fmt.Errorf("auctionsBySellerIDMap err: %v", err)
					}
					err = g.GroupByAndProduce(ctx, changeKeyedMsg[0], args.TrackParFunc())
					if err != nil {
						fmt.Fprintf(os.Stderr, "[ERROR] groupby failed: %v", err)
						errChan <- err
						return
					}
				}
			}
		}
	}
}
