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
	"sharedlog-stream/pkg/debug"
	"sharedlog-stream/pkg/execution"
	"sharedlog-stream/pkg/proc_interface"
	"sharedlog-stream/pkg/processor"
	"sharedlog-stream/pkg/producer_consumer"
	"sharedlog-stream/pkg/stream_task"
	"sync"
	"time"

	"cs.utexas.edu/zjia/faas/types"
)

type q3GroupByHandler struct {
	env types.Environment

	funcName string
}

func NewQ3GroupByHandler(env types.Environment, funcName string) types.FuncHandler {
	return &q3GroupByHandler{
		env:      env,
		funcName: funcName,
	}
}

func (h *q3GroupByHandler) procMsg(ctx context.Context, msg commtypes.Message, argsTmp interface{}) error {
	args := argsTmp.(*TwoMsgChanProcArgs)
	args.msgChan1 <- msg
	args.msgChan2 <- msg
	return nil
}

func (h *q3GroupByHandler) Call(ctx context.Context, input []byte) ([]byte, error) {
	parsedInput := &common.QueryInput{}
	err := json.Unmarshal(input, parsedInput)
	if err != nil {
		return nil, err
	}
	output := h.Q3GroupBy(ctx, parsedInput)
	encodedOutput, err := json.Marshal(output)
	if err != nil {
		panic(err)
	}
	return utils.CompressData(encodedOutput), nil
}

type TwoMsgChanProcArgs struct {
	msgChan1 chan commtypes.Message
	msgChan2 chan commtypes.Message
	proc_interface.BaseExecutionContext
}

func getSrcSinks(ctx context.Context, env types.Environment, sp *common.QueryInput,
) ([]producer_consumer.MeteredConsumerIntr, []producer_consumer.MeteredProducerIntr, error) {
	input_stream, output_streams, err := benchutil.GetShardedInputOutputStreams(ctx, env, sp)
	if err != nil {
		return nil, nil, err
	}
	debug.Assert(len(output_streams) == 2, "expected 2 output streams")
	var sinks []producer_consumer.MeteredProducerIntr
	serdeFormat := commtypes.SerdeFormat(sp.SerdeFormat)
	msgSerde, err := commtypes.GetMsgSerde(serdeFormat)
	if err != nil {
		return nil, nil, fmt.Errorf("get msg serde err: %v", err)
	}
	eventSerde, err := ntypes.GetEventSerde(serdeFormat)
	if err != nil {
		return nil, nil, fmt.Errorf("get event serde err: %v", err)
	}
	inConfig := &producer_consumer.StreamConsumerConfig{
		Timeout: common.SrcConsumeTimeout,
		KVMsgSerdes: commtypes.KVMsgSerdes{
			KeySerde: commtypes.StringSerde{},
			ValSerde: eventSerde,
			MsgSerde: msgSerde,
		},
	}
	outConfig := &producer_consumer.StreamSinkConfig{
		KVMsgSerdes: commtypes.KVMsgSerdes{
			KeySerde: commtypes.Uint64Serde{},
			ValSerde: eventSerde,
			MsgSerde: msgSerde,
		},
		FlushDuration: time.Duration(sp.FlushMs) * time.Millisecond,
	}
	// fmt.Fprintf(os.Stderr, "output to %v\n", output_stream.TopicName())
	warmup := time.Duration(sp.WarmupS) * time.Second
	src := producer_consumer.NewMeteredConsumer(producer_consumer.NewShardedSharedLogStreamConsumer(input_stream, inConfig), warmup)
	for _, output_stream := range output_streams {
		sink := producer_consumer.NewMeteredProducer(producer_consumer.NewShardedSharedLogStreamProducer(output_stream, outConfig),
			warmup)
		sinks = append(sinks, sink)
	}
	return []producer_consumer.MeteredConsumerIntr{src}, sinks, nil
}

func (h *q3GroupByHandler) Q3GroupBy(ctx context.Context, sp *common.QueryInput) *common.FnOutput {
	srcs, sinks_arr, err := getSrcSinks(ctx, h.env, sp)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	srcs[0].SetInitialSource(true)
	sinks_arr[0].SetName("aucSink")
	sinks_arr[1].SetName("perSink")
	warmup := time.Duration(sp.WarmupS) * time.Second

	filterPerson, personsByIDMap, personsByIDFunc := h.getPersonsByID(warmup)
	filterAuctions, auctionsBySellerIDMap, auctionsBySellerIDFunc := h.getAucBySellerID(warmup)

	var wg sync.WaitGroup
	personsByIDManager := execution.NewGeneralProcManager(personsByIDFunc)
	auctionsBySellerIDManager := execution.NewGeneralProcManager(auctionsBySellerIDFunc)
	srcsSinks := proc_interface.NewBaseSrcsSinks(srcs, sinks_arr)
	procArgs := &TwoMsgChanProcArgs{
		msgChan1: auctionsBySellerIDManager.MsgChan(),
		msgChan2: personsByIDManager.MsgChan(),
		BaseExecutionContext: proc_interface.NewExecutionContextFromComponents(srcsSinks,
			proc_interface.NewBaseProcArgs(h.funcName, sp.ScaleEpoch, sp.ParNum)),
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
		InitFunc(func(progArgsTmp interface{}) {
			filterPerson.StartWarmup()
			personsByIDMap.StartWarmup()
			filterAuctions.StartWarmup()
			auctionsBySellerIDMap.StartWarmup()
			// debug.Fprintf(os.Stderr, "done warmup start\n")
		}).
		HandleErrFunc(handleErrFunc).
		PauseFunc(func() *common.FnOutput {
			debug.Fprintf(os.Stderr, "begin flush\n")
			personsByIDManager.RequestToTerminate()
			auctionsBySellerIDManager.RequestToTerminate()
			wg.Wait()
			if err := handleErrFunc(); err != nil {
				return &common.FnOutput{Success: false, Message: err.Error()}
			}
			debug.Fprintf(os.Stderr, "done flush\n")
			return nil
		}).
		ResumeFunc(func(task *stream_task.StreamTask) {
			debug.Fprintf(os.Stderr, "start resume\n")
			personsByIDManager.RecreateMsgChan(&procArgs.msgChan2)
			auctionsBySellerIDManager.RecreateMsgChan(&procArgs.msgChan1)
			personsByIDManager.LaunchProc(ctx, procArgs, &wg)
			auctionsBySellerIDManager.LaunchProc(ctx, procArgs, &wg)
			debug.Fprintf(os.Stderr, "done resume\n")
		}).Build()

	update_stats := func(ret *common.FnOutput) {
		ret.Latencies["filterPerson"] = filterPerson.GetLatency()
		ret.Latencies["personsByIDMap"] = personsByIDMap.GetLatency()
		ret.Latencies["filterAuctions"] = filterAuctions.GetLatency()
		ret.Latencies["auctionsBySellerIDMap"] = auctionsBySellerIDMap.GetLatency()
	}
	transactionalID := fmt.Sprintf("%s-%s-%d",
		h.funcName, sp.InputTopicNames[0], sp.ParNum)
	streamTaskArgs := benchutil.UpdateStreamTaskArgs(sp,
		stream_task.NewStreamTaskArgsBuilder(h.env, procArgs, transactionalID)).Build()
	return task.ExecuteApp(ctx, streamTaskArgs, update_stats)
}

func (h *q3GroupByHandler) getPersonsByID(warmup time.Duration) (
	*processor.MeteredProcessor,
	*processor.MeteredProcessor,
	execution.GeneralProcFunc,
) {
	filterPerson := processor.NewMeteredProcessor(processor.NewStreamFilterProcessor(processor.PredicateFunc(
		func(msg *commtypes.Message) (bool, error) {
			event := msg.Value.(*ntypes.Event)
			return event.Etype == ntypes.PERSON && ((event.NewPerson.State == "OR") ||
				event.NewPerson.State == "ID" || event.NewPerson.State == "CA"), nil
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

	return filterPerson, personsByIDMap, func(ctx context.Context, argsTmp interface{}, wg *sync.WaitGroup, msgChan chan commtypes.Message, errChan chan error) {
		args := argsTmp.(*TwoMsgChanProcArgs)
		defer wg.Done()
		g := processor.NewGroupBy(args.Producers()[1])
	L:
		for {
			select {
			case <-ctx.Done():
				break L
			case msg, ok := <-msgChan:
				if !ok {
					break L
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
						fmt.Fprintf(os.Stderr, "[ERROR] fail groupby: %v\n", err)
						errChan <- err
						return
					}
				}
			}
		}
	}
}

func (h *q3GroupByHandler) getAucBySellerID(warmup time.Duration) (
	*processor.MeteredProcessor,
	*processor.MeteredProcessor,
	execution.GeneralProcFunc,
) {
	filterAuctions := processor.NewMeteredProcessor(processor.NewStreamFilterProcessor(processor.PredicateFunc(
		func(m *commtypes.Message) (bool, error) {
			event := m.Value.(*ntypes.Event)
			return event.Etype == ntypes.AUCTION && event.NewAuction.Category == 10, nil
		})), warmup)

	auctionsBySellerIDMap := processor.NewMeteredProcessor(processor.NewStreamMapProcessor(
		processor.MapperFunc(func(msg commtypes.Message) (commtypes.Message, error) {
			event := msg.Value.(*ntypes.Event)
			return commtypes.Message{Key: event.NewAuction.Seller, Value: msg.Value, Timestamp: msg.Timestamp}, nil
		})), warmup)

	return filterAuctions, auctionsBySellerIDMap,
		func(ctx context.Context, argsTmp interface{}, wg *sync.WaitGroup,
			msgChan chan commtypes.Message, errChan chan error,
		) {
			args := argsTmp.(*TwoMsgChanProcArgs)
			g := processor.NewGroupBy(args.Producers()[0])
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
						fmt.Fprintf(os.Stderr, "filterAuctions err: %v\n", err)
						errChan <- fmt.Errorf("filterAuctions err: %v", err)
						return
					}
					for _, filteredMsg := range filteredMsgs {
						changeKeyedMsg, err := auctionsBySellerIDMap.ProcessAndReturn(ctx, filteredMsg)
						if err != nil {
							fmt.Fprintf(os.Stderr, "[ERROR] auctionsBySellerIDMap err: %v\n", err)
							errChan <- fmt.Errorf("auctionsBySellerIDMap err: %v", err)
							return
						}

						err = g.GroupByAndProduce(ctx, changeKeyedMsg[0], args.TrackParFunc())
						if err != nil {
							fmt.Fprintf(os.Stderr, "[ERROR] fail groupby: %v\n", err)
							errChan <- err
							return
						}
					}
				}
			}
		}
}
