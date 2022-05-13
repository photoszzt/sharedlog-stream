package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/benchmark/common/benchutil"
	"sharedlog-stream/benchmark/nexmark/pkg/nexmark/utils"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/stream/processor"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"sharedlog-stream/pkg/transaction"

	ntypes "sharedlog-stream/benchmark/nexmark/pkg/nexmark/types"

	"cs.utexas.edu/zjia/faas/types"
)

type query2Handler struct {
	env      types.Environment
	funcName string
}

func NewQuery2(env types.Environment, funcName string) types.FuncHandler {
	return &query2Handler{
		env:      env,
		funcName: funcName,
	}
}

func (h *query2Handler) Call(ctx context.Context, input []byte) ([]byte, error) {
	parsedInput := &common.QueryInput{}
	err := json.Unmarshal(input, parsedInput)
	if err != nil {
		return nil, err
	}
	output := h.Query2(ctx, parsedInput)
	encodedOutput, err := json.Marshal(output)
	if err != nil {
		panic(err)
	}
	return utils.CompressData(encodedOutput), nil
}

func filterFunc(msg *commtypes.Message) (bool, error) {
	event := msg.Value.(*ntypes.Event)
	return event.Etype == ntypes.BID && event.Bid.Auction%123 == 0, nil
}

type query2ProcessArgs struct {
	src              *processor.MeteredSource
	sink             *sharedlog_stream.MeteredSink
	q2Filter         *processor.MeteredProcessor
	output_stream    *sharedlog_stream.ShardedSharedLogStream
	trackParFunc     transaction.TrackKeySubStreamFunc
	recordFinishFunc transaction.RecordPrevInstanceFinishFunc
	funcName         string
	curEpoch         uint64
	parNum           uint8
}

func (a *query2ProcessArgs) Source() processor.Source { return a.src }
func (a *query2ProcessArgs) PushToAllSinks(ctx context.Context, msg commtypes.Message, parNum uint8, isControl bool) error {
	return a.sink.Sink(ctx, msg, parNum, isControl)
}
func (a *query2ProcessArgs) ParNum() uint8    { return a.parNum }
func (a *query2ProcessArgs) CurEpoch() uint64 { return a.curEpoch }
func (a *query2ProcessArgs) FuncName() string { return a.funcName }
func (a *query2ProcessArgs) RecordFinishFunc() func(ctx context.Context, funcName string, instanceId uint8) error {
	return a.recordFinishFunc
}
func (a *query2ProcessArgs) ErrChan() chan error {
	return nil
}

func (h *query2Handler) Query2(ctx context.Context, sp *common.QueryInput) *common.FnOutput {
	input_stream, output_streams, err := benchutil.GetShardedInputOutputStreams(ctx, h.env, sp, false)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: fmt.Sprintf("get input output stream failed: %v", err),
		}
	}
	src, sink, msgSerde, err := getSrcSink(ctx, sp, input_stream, output_streams[0])
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: err.Error(),
		}
	}
	sink.MarkFinalOutput()
	q2Filter := processor.NewMeteredProcessor(processor.NewStreamFilterProcessor(processor.PredicateFunc(filterFunc)), time.Duration(sp.WarmupS)*time.Second)
	procArgs := &query2ProcessArgs{
		src:              src,
		sink:             sink,
		q2Filter:         q2Filter,
		output_stream:    output_streams[0],
		parNum:           sp.ParNum,
		trackParFunc:     transaction.DefaultTrackSubstreamFunc,
		recordFinishFunc: transaction.DefaultRecordPrevInstanceFinishFunc,
		funcName:         h.funcName,
		curEpoch:         sp.ScaleEpoch,
	}
	task := transaction.StreamTask{
		ProcessFunc:               h.process,
		CurrentOffset:             make(map[string]uint64),
		CommitEveryForAtLeastOnce: common.CommitDuration,
		PauseFunc: func() {
			err := sink.Flush(ctx)
			if err != nil {
				panic(err)
			}
		},
		InitFunc: func(progArgs interface{}) {
			if sp.EnableTransaction {
				sink.InnerSink().StartAsyncPushNoTick(ctx)
			} else {
				sink.InnerSink().StartAsyncPushWithTick(ctx)
				sink.InitFlushTimer()
			}
			src.StartWarmup()
			sink.StartWarmup()
			q2Filter.StartWarmup()
		},
		CloseFunc: func() {
			sink.CloseAsyncPush()
			if err = sink.Flush(ctx); err != nil {
				panic(err)
			}
		},
	}
	srcs := map[string]processor.Source{sp.InputTopicNames[0]: src}
	if sp.EnableTransaction {
		streamTaskArgs := transaction.StreamTaskArgsTransaction{
			ProcArgs:              procArgs,
			Env:                   h.env,
			MsgSerde:              msgSerde,
			Srcs:                  srcs,
			OutputStreams:         output_streams,
			QueryInput:            sp,
			TransactionalId:       fmt.Sprintf("%s-%s-%d-%s", h.funcName, sp.InputTopicNames[0], sp.ParNum, sp.OutputTopicNames[0]),
			KVChangelogs:          nil,
			WindowStoreChangelogs: nil,
			FixedOutParNum:        sp.ParNum,
		}
		ret := transaction.SetupManagersAndProcessTransactional(ctx, h.env, &streamTaskArgs,
			func(procArgs interface{}, trackParFunc transaction.TrackKeySubStreamFunc,
				recordFinishFunc transaction.RecordPrevInstanceFinishFunc) {
				procArgs.(*query2ProcessArgs).trackParFunc = trackParFunc
				procArgs.(*query2ProcessArgs).recordFinishFunc = recordFinishFunc
			}, &task)
		if ret != nil && ret.Success {
			ret.Latencies["src"] = src.GetLatency()
			ret.Latencies["sink"] = sink.GetLatency()
			ret.Latencies["q2Filter"] = q2Filter.GetLatency()
			ret.Latencies["eventTimeLatency"] = sink.GetEventTimeLatency()
			ret.Consumed["src"] = src.GetCount()
		}
		return ret
	}
	streamTaskArgs := transaction.StreamTaskArgs{
		ProcArgs:       procArgs,
		Duration:       time.Duration(sp.Duration) * time.Second,
		Srcs:           srcs,
		ParNum:         sp.ParNum,
		SerdeFormat:    commtypes.SerdeFormat(sp.SerdeFormat),
		Env:            h.env,
		NumInPartition: sp.NumInPartition,
		WarmupTime:     time.Duration(sp.WarmupS) * time.Second,
	}
	ret := task.Process(ctx, &streamTaskArgs)
	if ret != nil && ret.Success {
		ret.Latencies["src"] = src.GetLatency()
		ret.Latencies["sink"] = sink.GetLatency()
		ret.Latencies["q2Filter"] = q2Filter.GetLatency()
		ret.Latencies["eventTimeLatency"] = sink.GetEventTimeLatency()
		ret.Consumed["src"] = src.GetCount()
	}
	return ret
}

func (h *query2Handler) process(ctx context.Context, t *transaction.StreamTask, argsTmp interface{}) (map[string]uint64, *common.FnOutput) {
	args := argsTmp.(*query2ProcessArgs)
	return transaction.CommonProcess(ctx, t, args, func(t *transaction.StreamTask, msg commtypes.MsgAndSeq) error {
		t.CurrentOffset[args.src.TopicName()] = msg.LogSeqNum
		if msg.MsgArr != nil {
			for _, subMsg := range msg.MsgArr {
				if subMsg.Value == nil {
					continue
				}
				outMsg, err := args.q2Filter.ProcessAndReturn(ctx, subMsg)
				if err != nil {
					return err
				}
				if outMsg != nil {
					err = args.sink.Sink(ctx, outMsg[0], args.parNum, false)
					if err != nil {
						return err
					}
				}
			}
		} else {
			outMsg, err := args.q2Filter.ProcessAndReturn(ctx, msg.Msg)
			if err != nil {
				return err
			}
			if outMsg != nil {
				err = args.sink.Sink(ctx, outMsg[0], args.parNum, false)
				if err != nil {
					return err
				}
			}
		}
		return nil
	})
}

/*
func Query2(ctx context.Context, env types.Environment, input *common.QueryInput, output chan *common.FnOutput) {
	// fmt.Fprintf(os.Stderr, "input topic name is %v\n", input.InputTopicName)
	inputStream := sharedlog_stream.NewSharedLogStream(env, input.InputTopicName)
	outputStream := sharedlog_stream.NewSharedLogStream(env, input.OutputTopicName)
	msgSerde, err := commtypes.GetMsgSerde(input.SerdeFormat)
	if err != nil {
		output <- &common.FnOutput{
			Success: false,
			Message: err.Error(),
		}
	}
	eventSerde, err := getEventSerde(input.SerdeFormat)
	if err != nil {
		output <- &common.FnOutput{
			Success: false,
			Message: err.Error(),
		}
	}

	inConfig := &sharedlog_stream.SharedLogStreamConfig{
		Timeout:      time.Duration(input.Duration) * time.Second,
		KeyDecoder:   commtypes.StringDecoder{},
		ValueDecoder: eventSerde,
		MsgDecoder:   msgSerde,
	}

	outConfig := &sharedlog_stream.StreamSinkConfig{
		KeyEncoder:   commtypes.StringEncoder{},
		ValueEncoder: eventSerde,
		MsgEncoder:   msgSerde,
	}
	builder := stream.NewStreamBuilder()
	builder.Source("nexmark_src", sharedlog_stream.NewSharedLogStreamSource(inputStream, inConfig)).
		Filter("only_bid", processor.PredicateFunc(only_bid)).
		Filter("q2_filter", processor.PredicateFunc(filterFunc)).
		Process("sink", sharedlog_stream.NewSharedLogStreamSink(outputStream, outConfig))
	tp, err_arrs := builder.Build()
	if err_arrs != nil {
		output <- &common.FnOutput{
			Success: false,
			Message: fmt.Sprintf("build stream failed: %v", err_arrs),
		}
	}
	pumps := make(map[processor.Node]processor.Pump)
	var srcPumps []processor.SourcePump
	nodes := processor.FlattenNodeTree(tp.Sources())
	processor.ReverseNodes(nodes)
	for _, node := range nodes {
		pipe := processor.NewPipe(processor.ResolvePumps(pumps, node.Children()))
		node.Processor().WithPipe(pipe)

		pump := processor.NewSyncPump(node, pipe)
		pumps[node] = pump
	}
	for source, node := range tp.Sources() {
		srcPump := processor.NewSourcePump(ctx, node.Name(), source, 0,
			processor.ResolvePumps(pumps, node.Children()), func(err error) {
				log.Fatal(err.Error())
			})
		srcPumps = append(srcPumps, srcPump)
	}

	duration := time.Duration(input.Duration) * time.Second
	latencies := make([]int, 0, 128)
	startTime := time.Now()

	time.After(duration)
	for _, srcPump := range srcPumps {
		srcPump.Stop()
		srcPump.Close()
	}
	output <- &common.FnOutput{
		Success:   true,
		Duration:  time.Since(startTime).Seconds(),
		Latencies: map[string][]int{"e2e": latencies},
	}
}
*/
