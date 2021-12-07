package wordcount

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/benchmark/common/benchutil"
	"sharedlog-stream/benchmark/nexmark/pkg/nexmark/utils"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/stream/processor"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"sharedlog-stream/pkg/stream/processor/store"
	"time"

	"cs.utexas.edu/zjia/faas/types"
)

type wordcountCounterAgg struct {
	env types.Environment
}

func NewWordCountCounterAgg(env types.Environment) *wordcountCounterAgg {
	return &wordcountCounterAgg{
		env: env,
	}
}

func (h *wordcountCounterAgg) Call(ctx context.Context, input []byte) ([]byte, error) {
	sp := &common.QueryInput{}
	err := json.Unmarshal(input, sp)
	if err != nil {
		return nil, err
	}
	output := h.wordcount_counter(ctx, sp)
	encodedOutput, err := json.Marshal(output)
	if err != nil {
		return nil, err
	}
	return utils.CompressData(encodedOutput), nil
}

func setupCounter(ctx context.Context, sp *common.QueryInput, msgSerde commtypes.MsgSerde,
	output_stream store.Stream) (*processor.MeteredProcessor, error) {
	var vtSerde commtypes.Serde
	if sp.SerdeFormat == uint8(commtypes.JSON) {
		vtSerde = commtypes.ValueTimestampJSONSerde{
			ValJSONSerde: commtypes.Uint64Serde{},
		}
	} else if sp.SerdeFormat == uint8(commtypes.MSGP) {
		vtSerde = commtypes.ValueTimestampMsgpSerde{
			ValMsgpSerde: commtypes.Uint64Serde{},
		}
	} else {
		return nil, fmt.Errorf("serde format should be either json or msgp; but %v is given", sp.SerdeFormat)
	}
	mp := &store.MaterializeParam{
		KeySerde:   commtypes.StringSerde{},
		ValueSerde: vtSerde,
		MsgSerde:   msgSerde,
		StoreName:  sp.OutputTopicName,
		Changelog:  output_stream,
		ParNum:     sp.ParNum,
	}
	store := store.NewInMemoryKeyValueStoreWithChangelog(mp)
	// fmt.Fprintf(os.Stderr, "before restore\n")
	err := store.RestoreStateStore(ctx)
	if err != nil {
		return nil, fmt.Errorf("restore failed: %v", err)
	}
	p := processor.NewMeteredProcessor(processor.NewStreamAggregateProcessor(store,
		processor.InitializerFunc(func() interface{} {
			return uint64(0)
		}),
		processor.AggregatorFunc(func(key interface{}, value interface{}, agg interface{}) interface{} {
			aggVal := agg.(uint64)
			fmt.Fprintf(os.Stderr, "update %v count to %d\n", key, aggVal+1)
			return aggVal + 1
		})))
	return p, nil
}

type wordcountCounterAggProcessArg struct {
	src           *processor.MeteredSource
	output_stream *store.MeteredStream
	counter       *processor.MeteredProcessor
	parNum        uint8
}

func (h *wordcountCounterAgg) process(ctx context.Context,
	argsTmp interface{},
	trackParFunc func([]uint8) error,
) (uint64, *common.FnOutput) {
	args := argsTmp.(*wordcountCounterAggProcessArg)
	currentOffset := uint64(0)
	msgs, err := args.src.Consume(ctx, args.parNum)
	if err != nil {
		if errors.Is(err, sharedlog_stream.ErrStreamSourceTimeout) {
			return currentOffset, &common.FnOutput{
				Success: true,
				Message: err.Error(),
			}
		}
		return currentOffset, &common.FnOutput{
			Success: false,
			Message: fmt.Sprintf("consume failed: %v", err),
		}
	}

	for _, msg := range msgs {
		currentOffset = msg.LogSeqNum
		_, err = args.counter.ProcessAndReturn(ctx, msg.Msg)
		if err != nil {
			return currentOffset, &common.FnOutput{
				Success: false,
				Message: fmt.Sprintf("counter failed: %v", err),
			}
		}
	}
	return currentOffset, nil
}

/*
func (h *wordcountCounterAgg) processWithTranLoop(
	ctx context.Context,
	sp *common.QueryInput,
	args wordcountCounterAggProcessArg,
	tm *sharedlog_stream.TransactionManager, appId uint64, appEpoch uint16,
	retc chan *common.FnOutput,
) {
	latencies := make([]int, 0, 128)
	duration := time.Duration(sp.Duration) * time.Second

	hasLiveTransaction := false
	trackConsumePar := false
	currentOffset := uint64(0)

	var commitTimer time.Time
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
		isTimeToCommit := commitEvery != 0 && timeSinceTranStart > commitEvery
		if hasLiveTransaction && (isTimeToCommit || timeout) {
			sharedlog_stream.TrackOffsetAndCommit(ctx, sharedlog_stream.ConsumedSeqNumConfig{
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
					Message: fmt.Sprintf("add offsets failed: %v", err),
				}
			}
			trackConsumePar = true
		}
		procStart := time.Now()
		msgs, err := args.src.Consume(ctx, sp.ParNum)
		if err != nil {
			if errors.Is(err, sharedlog_stream.ErrStreamSourceTimeout) {
				elapsed := time.Since(procStart)
				latencies = append(latencies, int(elapsed.Microseconds()))
				retc <- &common.FnOutput{
					Success: true,
					Message: err.Error(),
					Latencies: map[string][]int{
						"e2e":           latencies,
						"src":           args.src.GetLatency(),
						"count":         args.counter.GetLatency(),
						"changelogRead": args.output_stream.GetReadNextLatencies(),
						"changelogPush": args.output_stream.GetPushLatencies(),
					},
					Duration: time.Since(startTime).Seconds(),
				}
			}
			retc <- &common.FnOutput{
				Success: false,
				Message: fmt.Sprintf("consume failed: %v", err),
			}
		}

		for _, msg := range msgs {
			currentOffset = msg.LogSeqNum
			_, err = args.counter.ProcessAndReturn(ctx, msg.Msg)
			if err != nil {
				retc <- &common.FnOutput{
					Success: false,
					Message: fmt.Sprintf("counter failed: %v", err),
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
			"e2e":           latencies,
			"src":           args.src.GetLatency(),
			"count":         args.counter.GetLatency(),
			"changelogRead": args.output_stream.GetReadNextLatencies(),
			"changelogPush": args.output_stream.GetPushLatencies(),
		},
	}
}

func (h *wordcountCounterAgg) processWithTransaction(ctx context.Context,
	sp *common.QueryInput,
	args wordcountCounterAggProcessArg,
) *common.FnOutput {

	// fmt.Fprintf(os.Stderr, "word count counter function enables exactly once semantics\n")
	transactionalId := fmt.Sprintf("wordcount-counter-%s-%s-%d", sp.InputTopicName, sp.OutputTopicName, sp.ParNum)
	tm, err := sharedlog_stream.SetupTransactionManager(ctx)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: fmt.Sprintf("setup transaction manager failed: %v", err),
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

func (h *wordcountCounterAgg) process(ctx context.Context, sp *common.QueryInput,
	args wordcountCounterAggProcessArg,
) *common.FnOutput {
	latencies := make([]int, 0, 128)
	duration := time.Duration(sp.Duration) * time.Second
	startTime := time.Now()
	for {
		if duration != 0 && time.Since(startTime) >= duration {
			break
		}
		procStart := time.Now()
		msgs, err := args.src.Consume(ctx, sp.ParNum)
		if err != nil {
			if errors.Is(err, sharedlog_stream.ErrStreamSourceTimeout) {
				elapsed := time.Since(procStart)
				latencies = append(latencies, int(elapsed.Microseconds()))
				return &common.FnOutput{
					Success: true,
					Message: err.Error(),
					Latencies: map[string][]int{
						"e2e":           latencies,
						"src":           args.src.GetLatency(),
						"count":         args.counter.GetLatency(),
						"changelogRead": args.output_stream.GetReadNextLatencies(),
						"changelogPush": args.output_stream.GetPushLatencies(),
					},
					Duration: time.Since(startTime).Seconds(),
				}
			}
			return &common.FnOutput{
				Success: false,
				Message: fmt.Sprintf("consume failed: %v", err),
			}
		}
		for _, msg := range msgs {
			_, err = args.counter.ProcessAndReturn(ctx, msg.Msg)
			if err != nil {
				return &common.FnOutput{
					Success: false,
					Message: fmt.Sprintf("counter failed: %v", err),
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
			"e2e":           latencies,
			"src":           args.src.GetLatency(),
			"count":         args.counter.GetLatency(),
			"changelogRead": args.output_stream.GetReadNextLatencies(),
			"changelogPush": args.output_stream.GetPushLatencies(),
		},
	}
}
*/

func (h *wordcountCounterAgg) wordcount_counter(ctx context.Context, sp *common.QueryInput) *common.FnOutput {
	input_stream, output_stream, err := benchutil.GetShardedInputOutputStreams(ctx, h.env, sp)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: fmt.Sprintf("get input output stream failed: %v", err),
		}
	}
	meteredOutputStream := store.NewMeteredStream(output_stream)

	msgSerde, err := commtypes.GetMsgSerde(sp.SerdeFormat)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: fmt.Sprintf("get msg serde failed: %v", err),
		}
	}
	inConfig := &sharedlog_stream.SharedLogStreamConfig{
		Timeout:      common.SrcConsumeTimeout,
		KeyDecoder:   commtypes.StringDecoder{},
		ValueDecoder: commtypes.StringDecoder{},
		MsgDecoder:   msgSerde,
	}
	src := processor.NewMeteredSource(sharedlog_stream.NewShardedSharedLogStreamSource(input_stream, inConfig))
	count, err := setupCounter(ctx, sp, msgSerde, meteredOutputStream)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: fmt.Sprintf("setup counter failed: %v", err),
		}
	}

	procArgs := &wordcountCounterAggProcessArg{
		src:           src,
		output_stream: meteredOutputStream,
		counter:       count,
		parNum:        sp.ParNum,
	}

	task := sharedlog_stream.StreamTask{
		ProcessFunc: h.process,
	}

	if sp.EnableTransaction {
		// return h.processWithTransaction(ctx, sp, args)
		streamTaskArgs := sharedlog_stream.StreamTaskArgsTransaction{
			ProcArgs:        procArgs,
			Env:             h.env,
			Src:             src,
			OutputStream:    output_stream,
			QueryInput:      sp,
			TransactionalId: fmt.Sprintf("wordcount-counter-%s-%s-%d", sp.InputTopicName, sp.OutputTopicName, sp.ParNum),
		}
		ret := task.ProcessWithTransaction(ctx, &streamTaskArgs)
		if ret != nil && ret.Success {
			ret.Latencies["src"] = src.GetLatency()
			ret.Latencies["count"] = count.GetLatency()
			ret.Latencies["changelogRead"] = meteredOutputStream.GetReadNextLatencies()
			ret.Latencies["changelogPush"] = meteredOutputStream.GetPushLatencies()
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
		ret.Latencies["count"] = count.GetLatency()
		ret.Latencies["changelogRead"] = meteredOutputStream.GetReadNextLatencies()
		ret.Latencies["changelogPush"] = meteredOutputStream.GetPushLatencies()
	}
	return ret
}
