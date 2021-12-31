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
	"sharedlog-stream/pkg/stream/processor/store"
	"time"

	"cs.utexas.edu/zjia/faas/types"
	"golang.org/x/xerrors"
)

type q5AuctionBids struct {
	env   types.Environment
	cHash *hash.ConsistentHash
}

/*
func hashSe(key *ntypes.StartEndTime) uint32 {
	h := fnv.New32a()
	h.Write([]byte(fmt.Sprintf("%v", key)))
	return h.Sum32()
}
*/

func NewQ5AuctionBids(env types.Environment) *q5AuctionBids {
	return &q5AuctionBids{
		env:   env,
		cHash: hash.NewConsistentHash(),
	}
}

func (h *q5AuctionBids) Call(ctx context.Context, input []byte) ([]byte, error) {
	sp := &common.QueryInput{}
	err := json.Unmarshal(input, sp)
	if err != nil {
		return nil, err
	}
	output := h.processQ5AuctionBids(ctx, sp)
	encodedOutput, err := json.Marshal(output)
	if err != nil {
		return nil, err
	}
	return utils.CompressData(encodedOutput), nil
}

func (h *q5AuctionBids) getSrcSink(ctx context.Context, sp *common.QueryInput,
	msgSerde commtypes.MsgSerde, input_stream *sharedlog_stream.ShardedSharedLogStream,
	output_stream *sharedlog_stream.ShardedSharedLogStream,
) (*processor.MeteredSource, *processor.MeteredSink, error) {
	var seSerde commtypes.Serde
	var aucIdCountSerde commtypes.Serde
	if sp.SerdeFormat == uint8(commtypes.JSON) {
		seSerde = ntypes.StartEndTimeJSONSerde{}
		aucIdCountSerde = ntypes.AuctionIdCountJSONSerde{}
	} else if sp.SerdeFormat == uint8(commtypes.MSGP) {
		seSerde = ntypes.StartEndTimeMsgpSerde{}
		aucIdCountSerde = ntypes.AuctionIdCountMsgpSerde{}
	} else {
		return nil, nil, fmt.Errorf("serde format should be either json or msgp; but %v is given", sp.SerdeFormat)
	}

	eventSerde, err := getEventSerde(sp.SerdeFormat)
	if err != nil {
		return nil, nil, err
	}

	inConfig := &sharedlog_stream.SharedLogStreamConfig{
		Timeout:      common.SrcConsumeTimeout,
		KeyDecoder:   commtypes.Uint64Serde{},
		ValueDecoder: eventSerde,
		MsgDecoder:   msgSerde,
	}
	outConfig := &sharedlog_stream.StreamSinkConfig{
		MsgEncoder:   msgSerde,
		KeyEncoder:   seSerde,
		ValueEncoder: aucIdCountSerde,
	}
	src := processor.NewMeteredSource(sharedlog_stream.NewShardedSharedLogStreamSource(input_stream, inConfig))
	sink := processor.NewMeteredSink(sharedlog_stream.NewShardedSharedLogStreamSink(output_stream, outConfig))
	return src, sink, nil
}

func (h *q5AuctionBids) getCountAggProc(sp *common.QueryInput, msgSerde commtypes.MsgSerde) (*processor.MeteredProcessor, error) {
	hopWindow := processor.NewTimeWindowsNoGrace(time.Duration(10) * time.Second).AdvanceBy(time.Duration(2) * time.Second)
	countStoreName := "auctionBidsCountStore"
	changelogName := countStoreName + "-changelog"
	changelog, err := sharedlog_stream.NewShardedSharedLogStream(h.env, changelogName, uint8(sp.NumOutPartition))
	if err != nil {
		return nil, fmt.Errorf("NewShardedSharedLogStream failed: %v", err)
	}
	countMp := &store.MaterializeParam{
		KeySerde:   commtypes.Uint64Serde{},
		ValueSerde: commtypes.Uint64Serde{},
		MsgSerde:   msgSerde,
		StoreName:  countStoreName,
		Changelog:  changelog,
	}
	countWindowStore, err := store.NewInMemoryWindowStoreWithChangelog(
		hopWindow.MaxSize()+hopWindow.GracePeriodMs(),
		hopWindow.MaxSize(), countMp,
	)
	if err != nil {
		return nil, err
	}
	countProc := processor.NewMeteredProcessor(processor.NewStreamWindowAggregateProcessor(countWindowStore,
		processor.InitializerFunc(func() interface{} { return 0 }),
		processor.AggregatorFunc(func(key, value, aggregate interface{}) interface{} {
			val := aggregate.(uint64)
			return val + 1
		}), hopWindow))
	return countProc, nil
}

type q5AuctionBidsProcessArg struct {
	countProc       *processor.MeteredProcessor
	groupByAuction  *processor.MeteredProcessor
	src             *processor.MeteredSource
	sink            *processor.MeteredSink
	output_stream   *sharedlog_stream.ShardedSharedLogStream
	parNum          uint8
	numOutPartition uint8
}

func (h *q5AuctionBids) process(ctx context.Context,
	argsTmp interface{},
	trackParFunc func([]uint8) error,
) (uint64, *common.FnOutput) {
	args := argsTmp.(*q5AuctionBidsProcessArg)
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
		countMsgs, err := args.countProc.ProcessAndReturn(ctx, msg.Msg)
		if err != nil {
			return currentOffset, &common.FnOutput{
				Success: false,
				Message: err.Error(),
			}
		}
		for _, countMsg := range countMsgs {
			changeKeyedMsg, err := args.groupByAuction.ProcessAndReturn(ctx, countMsg)
			if err != nil {
				return currentOffset, &common.FnOutput{
					Success: false,
					Message: err.Error(),
				}
			}
			// par := uint8(hashSe(changeKeyedMsg[0].Key.(*ntypes.StartEndTime)) % uint32(args.numOutPartition))
			parTmp, ok := h.cHash.Get(changeKeyedMsg[0].Key.(*ntypes.StartEndTime))
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
					Message: err.Error(),
				}
			}
		}
	}
	return currentOffset, nil
}

/*
func (h *q5AuctionBids) process(ctx context.Context,
	sp *common.QueryInput, args q5AuctionBidsProcessArg,
) *common.FnOutput {
	duration := time.Duration(sp.Duration) * time.Second
	latencies := make([]int, 0, 128)
	startTime := time.Now()
	for {
		if duration != 0 && time.Since(startTime) >= duration {
			break
		}
		procStart := time.Now()
		msgs, err := args.src.Consume(ctx, sp.ParNum)
		if err != nil {
			if errors.Is(err, sharedlog_stream.ErrStreamSourceTimeout) {
				return &common.FnOutput{
					Success:  true,
					Message:  err.Error(),
					Duration: time.Since(startTime).Seconds(),
					Latencies: map[string][]int{
						"e2e":       latencies,
						"count":     args.countProc.GetLatency(),
						"changeKey": args.groupByAuction.GetLatency(),
						"src":       args.src.GetLatency(),
						"sink":      args.sink.GetLatency(),
					},
				}
			}
			return &common.FnOutput{
				Success: false,
				Message: err.Error(),
			}
		}
		for _, msg := range msgs {
			countMsgs, err := args.countProc.ProcessAndReturn(ctx, msg.Msg)
			if err != nil {
				return &common.FnOutput{
					Success: false,
					Message: err.Error(),
				}
			}
			for _, countMsg := range countMsgs {
				changeKeyedMsg, err := args.groupByAuction.ProcessAndReturn(ctx, countMsg)
				if err != nil {
					return &common.FnOutput{
						Success: false,
						Message: err.Error(),
					}
				}
				par := uint8(hashSe(changeKeyedMsg[0].Key.(*ntypes.StartEndTime)) % uint32(sp.NumOutPartition))
				err = args.sink.Sink(ctx, changeKeyedMsg[0], par, false)
				if err != nil {
					return &common.FnOutput{
						Success: false,
						Message: err.Error(),
					}
				}
			}
		}
		elapsed := time.Since(procStart)
		latencies = append(latencies, int(elapsed.Microseconds()))
	}
	return &common.FnOutput{
		Success:  true,
		Duration: time.Since(startTime).Seconds(),
		Latencies: map[string][]int{
			"e2e":       latencies,
			"count":     args.countProc.GetLatency(),
			"changeKey": args.groupByAuction.GetLatency(),
			"src":       args.src.GetLatency(),
			"sink":      args.sink.GetLatency(),
		},
	}
}

func (h *q5AuctionBids) processWithTranLoop(ctx context.Context,
	sp *common.QueryInput, args q5AuctionBidsProcessArg,
	tm *sharedlog_stream.TransactionManager, appId uint64, appEpoch uint16,
	retc chan *common.FnOutput,
) {
	duration := time.Duration(sp.Duration) * time.Second
	latencies := make([]int, 0, 128)
	hasLiveTransaction := false
	trackConsumePar := false
	currentOffset := uint64(0)
	commitTimer := time.Now()
	commitEvery := time.Duration(sp.CommitEvery) * time.Millisecond

	startTime := time.Now()

L:
	for {
		select {
		case <-ctx.Done():
			break L
		default:
		}

		timeSinceTranStart := time.Since(commitTimer)
		timeout := duration != 0 && time.Since(startTime) >= duration
		if (commitEvery != 0 && timeSinceTranStart > commitEvery) || timeout {
			benchutil.TrackOffsetAndCommit(ctx, sharedlog_stream.ConsumedSeqNumConfig{
				TopicToTrack:   sp.InputTopicName,
				TaskId:         appId,
				TaskEpoch:      appEpoch,
				Partition:      sp.ParNum,
				ConsumedSeqNum: currentOffset,
			}, tm, &hasLiveTransaction, &trackConsumePar, retc)
		}
		if timeout {
			err := tm.Close()
			if err != nil {
				retc <- &common.FnOutput{
					Success: false,
					Message: fmt.Sprintf("close transaction manager: %v\n", err),
				}
			}
			break
		}
		if !hasLiveTransaction {
			err := tm.BeginTransaction(ctx)
			if err != nil {
				retc <- &common.FnOutput{
					Success: false,
					Message: fmt.Sprintf("transaction begin failed: %v\n", err),
				}
			}
			hasLiveTransaction = true
			commitTimer = time.Now()
		}

		if !trackConsumePar {
			err = tm.AddTopicTrackConsumedSeqs(ctx, sp.InputTopicName, []uint8{sp.ParNum})
			if err != nil {
				retc <- &common.FnOutput{
					Success: false,
					Message: fmt.Sprintf("add offsets failed: %v\n", err),
				}
			}
			trackConsumePar = true
		}

		procStart := time.Now()
		gotMsgs, err := args.src.Consume(ctx, sp.ParNum)
		if err != nil {
			if xerrors.Is(err, sharedlog_stream.ErrStreamSourceTimeout) {
				retc <- &common.FnOutput{
					Success:  true,
					Message:  err.Error(),
					Duration: time.Since(startTime).Seconds(),
					Latencies: map[string][]int{
						"e2e":       latencies,
						"count":     args.countProc.GetLatency(),
						"changeKey": args.groupByAuction.GetLatency(),
						"src":       args.src.GetLatency(),
						"sink":      args.sink.GetLatency(),
					},
				}
			}
			retc <- &common.FnOutput{
				Success: false,
				Message: err.Error(),
			}
		}

		for _, msg := range gotMsgs {
			if msg.Msg.Value == nil {
				continue
			}
			currentOffset = msg.LogSeqNum
			countMsgs, err := args.countProc.ProcessAndReturn(ctx, msg.Msg)
			if err != nil {
				retc <- &common.FnOutput{
					Success: false,
					Message: err.Error(),
				}
			}
			for _, countMsg := range countMsgs {
				changeKeyedMsg, err := args.groupByAuction.ProcessAndReturn(ctx, countMsg)
				if err != nil {
					retc <- &common.FnOutput{
						Success: false,
						Message: err.Error(),
					}
				}
				par := uint8(hashSe(changeKeyedMsg[0].Key.(*ntypes.StartEndTime)) % uint32(sp.NumOutPartition))
				err = tm.AddTopicPartition(ctx, args.output_stream.TopicName(), []uint8{par})
				if err != nil {
					retc <- &common.FnOutput{
						Success: false,
						Message: fmt.Sprintf("add topic partition failed: %v\n", err),
					}
				}
				err = args.sink.Sink(ctx, changeKeyedMsg[0], par, false)
				if err != nil {
					retc <- &common.FnOutput{
						Success: false,
						Message: err.Error(),
					}
				}
			}
		}
		elapsed := time.Since(procStart)
		latencies = append(latencies, int(elapsed.Microseconds()))
	}
	retc <- &common.FnOutput{
		Success:  true,
		Duration: time.Since(startTime).Seconds(),
		Latencies: map[string][]int{
			"e2e":       latencies,
			"count":     args.countProc.GetLatency(),
			"changeKey": args.groupByAuction.GetLatency(),
			"src":       args.src.GetLatency(),
			"sink":      args.sink.GetLatency(),
		},
	}
}

func (h *q5AuctionBids) processWithTransaction(
	ctx context.Context, sp *common.QueryInput, args q5AuctionBidsProcessArg,
) *common.FnOutput {
	transactionalId := fmt.Sprintf("q5AuctionBids-%s-%d-%s", sp.InputTopicName, sp.ParNum, sp.OutputTopicName)
	tm, appId, appEpoch, err := benchutil.SetupTransactionManager(ctx, h.env, transactionalId, sp, args.src)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: err.Error(),
		}
	}
	tm.RecordTopicStreams(sp.OutputTopicName, args.output_stream)
	monitorQuit := make(chan struct{})
	monitorErrc := make(chan error)

	dctx, dcancel := context.WithCancel(ctx)
	go tm.MonitorTransactionLog(ctx, monitorQuit, monitorErrc, dcancel)

	retc := make(chan *common.FnOutput)
	go h.processWithTranLoop(dctx, sp, args, tm, appId, appEpoch, retc)
	for {
		select {
		case ret := <-retc:
			close(monitorQuit)
			return ret
		case merr := <-monitorErrc:
			close(monitorQuit)
			if merr != nil {
				return &common.FnOutput{
					Success: false,
					Message: fmt.Sprintf("monitor failed: %v", merr),
				}
			}
		}
	}
}
*/

func (h *q5AuctionBids) processQ5AuctionBids(ctx context.Context, sp *common.QueryInput) *common.FnOutput {
	msgSerde, err := commtypes.GetMsgSerde(sp.SerdeFormat)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: err.Error(),
		}
	}
	input_stream, output_stream, err := benchutil.GetShardedInputOutputStreams(ctx, h.env, sp)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: err.Error(),
		}
	}
	src, sink, err := h.getSrcSink(ctx, sp, msgSerde, input_stream, output_stream)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: err.Error(),
		}
	}
	countProc, err := h.getCountAggProc(sp, msgSerde)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: err.Error(),
		}
	}
	groupByAuction := processor.NewMeteredProcessor(processor.NewStreamMapProcessor(processor.MapperFunc(
		func(msg commtypes.Message) (commtypes.Message, error) {
			key := msg.Key.(*commtypes.WindowedKey)
			value := msg.Value.(uint64)
			newKey := &ntypes.StartEndTime{
				StartTime: key.Window.Start(),
				EndTime:   key.Window.End(),
			}
			newVal := &ntypes.AuctionIdCount{
				AucId: key.Key.(uint64),
				Count: value,
			}
			return commtypes.Message{Key: newKey, Value: newVal, Timestamp: msg.Timestamp}, nil
		})))
	procArgs := &q5AuctionBidsProcessArg{
		countProc:       countProc,
		groupByAuction:  groupByAuction,
		src:             src,
		sink:            sink,
		output_stream:   output_stream,
		parNum:          sp.ParNum,
		numOutPartition: sp.NumOutPartition,
	}

	task := sharedlog_stream.StreamTask{
		ProcessFunc: h.process,
	}

	for i := 0; i < int(sp.NumOutPartition); i++ {
		h.cHash.Add(uint8(i))
	}

	if sp.EnableTransaction {
		// return h.processWithTransaction(ctx, sp, args)
		streamTaskArgs := sharedlog_stream.StreamTaskArgsTransaction{
			ProcArgs:        procArgs,
			Env:             h.env,
			Src:             src,
			OutputStream:    output_stream,
			QueryInput:      sp,
			TransactionalId: fmt.Sprintf("q5AuctionBids-%s-%d-%s", sp.InputTopicName, sp.ParNum, sp.OutputTopicName),
			FixedOutParNum:  0,
		}
		ret := task.ProcessWithTransaction(ctx, &streamTaskArgs)
		if ret != nil && ret.Success {
			ret.Latencies["src"] = src.GetLatency()
			ret.Latencies["sink"] = sink.GetLatency()
			ret.Latencies["count"] = countProc.GetLatency()
			ret.Latencies["changeKey"] = groupByAuction.GetLatency()
		}
		return ret
	}
	// return h.process(ctx, sp, args)
	streamTaskArgs := sharedlog_stream.StreamTaskArgs{
		ProcArgs: procArgs,
		Duration: time.Duration(sp.Duration) * time.Second,
	}
	ret := task.Process(ctx, &streamTaskArgs)
	if ret != nil && ret.Success {
		ret.Latencies["src"] = src.GetLatency()
		ret.Latencies["sink"] = sink.GetLatency()
		ret.Latencies["count"] = countProc.GetLatency()
		ret.Latencies["changeKey"] = groupByAuction.GetLatency()
	}
	return ret
}
