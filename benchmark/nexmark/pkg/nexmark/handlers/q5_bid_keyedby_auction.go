package handlers

import (
	"context"
	"encoding/json"
	"errors"
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
)

type bidKeyedByAuction struct {
	env   types.Environment
	cHash *hash.ConsistentHash
}

func NewBidKeyedByAuctionHandler(env types.Environment) types.FuncHandler {
	return &bidKeyedByAuction{
		env:   env,
		cHash: hash.NewConsistentHash(),
	}
}

func (h *bidKeyedByAuction) Call(ctx context.Context, input []byte) ([]byte, error) {
	sp := &common.QueryInput{}
	err := json.Unmarshal(input, sp)
	if err != nil {
		return nil, err
	}
	output := h.processBidKeyedByAuction(ctx, sp)
	encodedOutput, err := json.Marshal(output)
	if err != nil {
		return nil, err
	}
	return utils.CompressData(encodedOutput), nil
}

func (h *bidKeyedByAuction) getSrcSink(
	sp *common.QueryInput,
	input_stream *sharedlog_stream.ShardedSharedLogStream,
	output_stream *sharedlog_stream.ShardedSharedLogStream,
) (*processor.MeteredSource, *processor.MeteredSink, error) {
	msgSerde, err := commtypes.GetMsgSerde(sp.SerdeFormat)
	if err != nil {
		return nil, nil, err
	}
	eventSerde, err := getEventSerde(sp.SerdeFormat)
	if err != nil {
		return nil, nil, err
	}
	inConfig := &sharedlog_stream.SharedLogStreamConfig{
		Timeout:      common.SrcConsumeTimeout,
		MsgDecoder:   msgSerde,
		KeyDecoder:   commtypes.StringDecoder{},
		ValueDecoder: eventSerde,
	}
	outConfig := &sharedlog_stream.StreamSinkConfig{
		MsgEncoder:   msgSerde,
		ValueEncoder: eventSerde,
		KeyEncoder:   commtypes.Uint64Encoder{},
	}

	src := processor.NewMeteredSource(sharedlog_stream.NewShardedSharedLogStreamSource(input_stream, inConfig))
	sink := processor.NewMeteredSink(sharedlog_stream.NewShardedSharedLogStreamSink(output_stream, outConfig))
	return src, sink, nil
}

type bidKeyedByAuctionProcessArgs struct {
	src             *processor.MeteredSource
	sink            *processor.MeteredSink
	filterBid       *processor.MeteredProcessor
	selectKey       *processor.MeteredProcessor
	output_stream   *sharedlog_stream.ShardedSharedLogStream
	parNum          uint8
	numOutPartition uint8
}

func (h *bidKeyedByAuction) process(ctx context.Context,
	argsTmp interface{},
	trackParFunc func([]uint8) error,
) (uint64, *common.FnOutput) {
	currentOffset := uint64(0)
	args := argsTmp.(*bidKeyedByAuctionProcessArgs)
	gotMsgs, err := args.src.Consume(ctx, args.parNum)
	if err != nil {
		if errors.Is(err, sharedlog_stream.ErrStreamSourceTimeout) {
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
		bidMsg, err := args.filterBid.ProcessAndReturn(ctx, msg.Msg)
		if err != nil {
			return currentOffset, &common.FnOutput{
				Success: false,
				Message: err.Error(),
			}
		}
		if bidMsg != nil {
			mappedKey, err := args.selectKey.ProcessAndReturn(ctx, bidMsg[0])
			if err != nil {
				return currentOffset, &common.FnOutput{
					Success: false,
					Message: err.Error(),
				}
			}
			key := mappedKey[0].Key.(uint64)
			parTmp, ok := h.cHash.Get(key)
			if !ok {
				return currentOffset, &common.FnOutput{
					Success: false,
					Message: "fail to calculate partition",
				}
			}
			par := parTmp.(uint8)
			// par := uint8(key % uint64(args.numOutPartition))
			err = trackParFunc([]uint8{par})
			if err != nil {
				return currentOffset, &common.FnOutput{
					Success: false,
					Message: fmt.Sprintf("add topic partition failed: %v\n", err),
				}
			}
			err = args.sink.Sink(ctx, mappedKey[0], par, false)
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
func (h *bidKeyedByAuction) process(ctx context.Context, sp *common.QueryInput, args bidKeyedByAuctionProcessArgs) *common.FnOutput {
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
						"src":       args.src.GetLatency(),
						"sink":      args.sink.GetLatency(),
						"filterBid": args.filterBid.GetLatency(),
						"selectKey": args.selectKey.GetLatency(),
					},
				}
			}
			return &common.FnOutput{
				Success: false,
				Message: err.Error(),
			}
		}

		for _, msg := range msgs {
			bidMsg, err := args.filterBid.ProcessAndReturn(ctx, msg.Msg)
			if err != nil {
				return &common.FnOutput{
					Success: false,
					Message: err.Error(),
				}
			}
			if bidMsg != nil {
				mappedKey, err := args.selectKey.ProcessAndReturn(ctx, bidMsg[0])
				if err != nil {
					return &common.FnOutput{
						Success: false,
						Message: err.Error(),
					}
				}
				key := mappedKey[0].Key.(uint64)
				par := uint8(key % uint64(sp.NumOutPartition))
				err = args.sink.Sink(ctx, mappedKey[0], par, false)
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
			"src":       args.src.GetLatency(),
			"sink":      args.sink.GetLatency(),
			"filterBid": args.filterBid.GetLatency(),
			"selectKey": args.selectKey.GetLatency(),
		},
	}
}

func (h *bidKeyedByAuction) processWithTranLoop(ctx context.Context, sp *common.QueryInput,
	args bidKeyedByAuctionProcessArgs, tm *sharedlog_stream.TransactionManager,
	appId uint64, appEpoch uint16, retc chan *common.FnOutput,
) {
	duration := time.Duration(sp.Duration) * time.Second
	latencies := make([]int, 0, 128)
	hasLiveTransaction := false
	trackConsumePar := false
	currentSeqNum := uint64(0)
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
				ConsumedSeqNum: currentSeqNum,
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
			if errors.Is(err, sharedlog_stream.ErrStreamSourceTimeout) {
				retc <- &common.FnOutput{
					Success:  true,
					Message:  err.Error(),
					Duration: time.Since(startTime).Seconds(),
					Latencies: map[string][]int{
						"e2e":       latencies,
						"src":       args.src.GetLatency(),
						"sink":      args.sink.GetLatency(),
						"filterBid": args.filterBid.GetLatency(),
						"selectKey": args.selectKey.GetLatency(),
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
			currentSeqNum = msg.LogSeqNum
			bidMsg, err := args.filterBid.ProcessAndReturn(ctx, msg.Msg)
			if err != nil {
				retc <- &common.FnOutput{
					Success: false,
					Message: err.Error(),
				}
			}
			if bidMsg != nil {
				mappedKey, err := args.selectKey.ProcessAndReturn(ctx, bidMsg[0])
				if err != nil {
					retc <- &common.FnOutput{
						Success: false,
						Message: err.Error(),
					}
				}
				key := mappedKey[0].Key.(uint64)
				par := uint8(key % uint64(sp.NumOutPartition))
				err = tm.AddTopicPartition(ctx, args.output_stream.TopicName(), []uint8{par})
				if err != nil {
					retc <- &common.FnOutput{
						Success: false,
						Message: fmt.Sprintf("add topic partition failed: %v\n", err),
					}
				}
				err = args.sink.Sink(ctx, mappedKey[0], par, false)
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
			"src":       args.src.GetLatency(),
			"sink":      args.sink.GetLatency(),
			"filterBid": args.filterBid.GetLatency(),
			"selectKey": args.selectKey.GetLatency(),
		},
	}
}

func (h *bidKeyedByAuction) processWithTransaction(ctx context.Context, sp *common.QueryInput, args bidKeyedByAuctionProcessArgs) *common.FnOutput {
	transactionalId := fmt.Sprintf("bidKeyedByAuction-%s-%d-%s", sp.InputTopicName, sp.ParNum, sp.OutputTopicName)
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

func (h *bidKeyedByAuction) processBidKeyedByAuction(ctx context.Context,
	sp *common.QueryInput,
) *common.FnOutput {
	input_stream, output_stream, err := benchutil.GetShardedInputOutputStreams(ctx, h.env, sp)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: err.Error(),
		}
	}

	src, sink, err := h.getSrcSink(sp, input_stream, output_stream)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: err.Error(),
		}
	}
	filterBid := processor.NewMeteredProcessor(processor.NewStreamFilterProcessor(processor.PredicateFunc(
		func(m *commtypes.Message) (bool, error) {
			event := m.Value.(*ntypes.Event)
			return event.Etype == ntypes.BID, nil
		})))
	selectKey := processor.NewMeteredProcessor(processor.NewStreamMapProcessor(processor.MapperFunc(
		func(m commtypes.Message) (commtypes.Message, error) {
			event := m.Value.(*ntypes.Event)
			return commtypes.Message{Key: event.Bid.Auction, Value: m.Value, Timestamp: m.Timestamp}, nil
		})))

	procArgs := &bidKeyedByAuctionProcessArgs{
		src:             src,
		sink:            sink,
		filterBid:       filterBid,
		selectKey:       selectKey,
		output_stream:   output_stream,
		parNum:          sp.ParNum,
		numOutPartition: sp.NumOutPartition,
	}

	task := sharedlog_stream.StreamTask{
		ProcessFunc: h.process,
	}

	for i := 0; i < int(sp.NumOutPartition); i++ {
		h.cHash.Add(i)
	}
	if sp.EnableTransaction {
		// return h.processWithTransaction(ctx, sp, args)
		streamTaskArgs := sharedlog_stream.StreamTaskArgsTransaction{
			ProcArgs:        procArgs,
			Env:             h.env,
			Src:             src,
			OutputStream:    output_stream,
			QueryInput:      sp,
			TransactionalId: fmt.Sprintf("bidKeyedByAuction-%s-%d-%s", sp.InputTopicName, sp.ParNum, sp.OutputTopicName),
			FixedOutParNum:  0,
		}
		ret := task.ProcessWithTransaction(ctx, &streamTaskArgs)
		if ret != nil && ret.Success {
			ret.Latencies["src"] = src.GetLatency()
			ret.Latencies["sink"] = sink.GetLatency()
			ret.Latencies["filterBid"] = filterBid.GetLatency()
			ret.Latencies["selectKey"] = selectKey.GetLatency()
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
		ret.Latencies["filterBid"] = filterBid.GetLatency()
		ret.Latencies["selectKey"] = selectKey.GetLatency()
	}
	return ret
}
