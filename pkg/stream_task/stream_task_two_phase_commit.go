package stream_task

import (
	"context"
	"fmt"
	"os"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/control_channel"
	"sharedlog-stream/pkg/debug"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/stats"
	"sharedlog-stream/pkg/transaction"
	"sync"
	"time"
)

func setupManagersFor2pc(ctx context.Context, t *StreamTask,
	streamTaskArgs *StreamTaskArgs,
) (*transaction.TransactionManager, *control_channel.ControlChannelManager, error) {
	debug.Fprint(os.Stderr, "setup transaction and control manager\n")
	tm, err := SetupTransactionManager(ctx, streamTaskArgs)
	if err != nil {
		return nil, nil, err
	}
	cmm, err := control_channel.NewControlChannelManager(streamTaskArgs.env, streamTaskArgs.appId,
		commtypes.SerdeFormat(streamTaskArgs.serdeFormat), streamTaskArgs.ectx.CurEpoch())
	if err != nil {
		return nil, nil, err
	}
	debug.Fprint(os.Stderr, "start restore\n")
	offsetMap, err := getOffsetMap(ctx, tm, streamTaskArgs)
	if err != nil {
		return nil, nil, err
	}
	debug.Fprintf(os.Stderr, "got offset map: %v\n", offsetMap)
	err = configChangelogExactlyOnce(tm, streamTaskArgs)
	if err != nil {
		return nil, nil, err
	}
	err = restoreStateStore(ctx, streamTaskArgs, offsetMap)
	if err != nil {
		return nil, nil, err
	}
	setOffsetOnStream(offsetMap, streamTaskArgs)
	debug.Fprintf(os.Stderr, "down restore\n")
	trackParFunc := func(ctx context.Context,
		key interface{},
		keySerde commtypes.Encoder,
		topicName string,
		substreamId uint8,
	) error {
		err := tm.AddTopicSubstream(ctx, topicName, substreamId)
		if err != nil {
			return err
		}
		err = control_channel.TrackAndAppendKeyMapping(ctx, cmm, key, keySerde, substreamId, topicName)
		return err
	}
	recordFinish := func(ctx context.Context, funcName string, instanceID uint8) error {
		return cmm.RecordPrevInstanceFinish(ctx, funcName, instanceID, cmm.CurrentEpoch())
	}
	updateFuncs(streamTaskArgs, trackParFunc, recordFinish)
	return tm, cmm, nil
}

func SetupTransactionManager(
	ctx context.Context,
	args *StreamTaskArgs,
) (*transaction.TransactionManager, error) {
	tm, err := transaction.NewTransactionManager(ctx, args.env,
		args.transactionalId, args.serdeFormat)
	if err != nil {
		return nil, fmt.Errorf("NewTransactionManager failed: %v", err)
	}
	err = tm.InitTransaction(ctx)
	if err != nil {
		return nil, fmt.Errorf("InitTransaction failed: %v", err)
	}

	return tm, nil
}

func processWithTransaction(
	ctx context.Context,
	t *StreamTask,
	tm *transaction.TransactionManager,
	cmm *control_channel.ControlChannelManager,
	args *StreamTaskArgs,
) *common.FnOutput {
	err := trackStreamAndConfigureExactlyOnce(args, tm,
		func(name string, stream *sharedlog_stream.ShardedSharedLogStream) {
			tm.RecordTopicStreams(name, stream)
			cmm.TrackStream(name, stream)
		})
	if err != nil {
		return common.GenErrFnOutput(err)
	}

	dctx, dcancel := context.WithCancel(ctx)
	tm.StartMonitorLog(dctx, dcancel)
	cmm.StartMonitorControlChannel(dctx)

	run := false

	latencies := stats.NewInt64Collector("latPerIter", stats.DEFAULT_COLLECT_DURATION)
	hasLiveTransaction := false
	trackConsumePar := false
	paused := false
	commitTimer := time.Now()

	debug.Fprintf(os.Stderr, "commit every: %v, guarantee: %v\n", args.commitEvery, args.guarantee)
	init := false
	var once sync.Once
	warmupCheck := stats.NewWarmupChecker(args.warmup)
	for {
		ret := checkMonitorReturns(dctx, dcancel, args, cmm, tm, &run)
		if ret != nil {
			return ret
		}
		if run {
			procStart := time.Now()
			once.Do(func() {
				warmupCheck.StartWarmup()
			})
			warmupCheck.Check()
			timeSinceTranStart := time.Since(commitTimer)
			cur_elapsed := warmupCheck.ElapsedSinceInitial()
			timeout := args.duration != 0 && cur_elapsed >= args.duration
			shouldCommitByTime := (args.commitEvery != 0 && timeSinceTranStart > args.commitEvery)
			// debug.Fprintf(os.Stderr, "shouldCommitByTime: %v, timeSinceTranStart: %v, cur_elapsed: %v\n",
			// 	shouldCommitByTime, timeSinceTranStart, cur_elapsed)

			// should commit
			if (shouldCommitByTime || timeout) && hasLiveTransaction {
				err_out := commitTransaction(ctx, t, tm, args, &hasLiveTransaction,
					&trackConsumePar, &paused)
				if err_out != nil {
					return err_out
				}
				debug.Assert(!hasLiveTransaction, "after commit. there should be no live transaction\n")
				// debug.Fprintf(os.Stderr, "transaction committed\n")
			}

			// Exit routine
			cur_elapsed = warmupCheck.ElapsedSinceInitial()
			timeout = args.duration != 0 && cur_elapsed >= args.duration
			if timeout {
				if err := tm.Close(); err != nil {
					debug.Fprintf(os.Stderr, "[ERROR] close transaction manager: %v\n", err)
					return &common.FnOutput{Success: false, Message: fmt.Sprintf("close transaction manager: %v\n", err)}
				}
				elapsed := time.Since(procStart)
				latencies.AddSample(elapsed.Microseconds())
				ret := &common.FnOutput{
					Success: true,
				}
				updateReturnMetric(ret, &warmupCheck)
				return ret
			}

			// begin new transaction
			err := startNewTransaction(ctx, t, args, tm, &hasLiveTransaction,
				&trackConsumePar, &init, &paused, &commitTimer)
			if err != nil {
				return &common.FnOutput{Success: false, Message: err.Error()}
			}
			debug.Assert(hasLiveTransaction, "after start new transaction. there should be a live transaction\n")

			ret := t.appProcessFunc(ctx, t, args.ectx)
			if ret != nil {
				if ret.Success {
					// consume timeout but not sure whether there's more data is coming; continue to process
					continue
				} else {
					if hasLiveTransaction {
						if err := tm.AbortTransaction(ctx); err != nil {
							return &common.FnOutput{Success: false, Message: fmt.Sprintf("abort failed: %v\n", err)}
						}
					}
				}
				return ret
			}
			if warmupCheck.AfterWarmup() {
				elapsed := time.Since(procStart)
				latencies.AddSample(elapsed.Microseconds())
			}
		}
	}
}

func startNewTransaction(ctx context.Context, t *StreamTask,
	args *StreamTaskArgs, tm *transaction.TransactionManager,
	hasLiveTransaction *bool, trackConsumePar *bool, init *bool, paused *bool, commitTimer *time.Time,
) error {
	if !*hasLiveTransaction {
		begTrStart := stats.TimerBegin()
		if err := tm.BeginTransaction(ctx); err != nil {
			debug.Fprintf(os.Stderr, "[ERROR] transaction begin failed: %v", err)
			return fmt.Errorf("transaction begin failed: %v\n", err)
		}
		begTrElapsed := stats.Elapsed(begTrStart).Microseconds()
		t.beginTrTime.AddSample(begTrElapsed)
		*hasLiveTransaction = true
		*commitTimer = time.Now()
		debug.Fprintf(os.Stderr, "fixedOutParNum: %d\n", args.fixedOutParNum)
		err := initAfterMarkOrCommit(ctx, t, args, tm, init, paused)
		if err != nil {
			return err
		}
	}
	if !*trackConsumePar {
		for _, src := range args.ectx.Consumers() {
			if err := tm.AddTopicTrackConsumedSeqs(ctx, src.TopicName(), args.ectx.SubstreamNum()); err != nil {
				return fmt.Errorf("add offsets failed: %v\n", err)
			}
		}
		*trackConsumePar = true
	}
	return nil
}

func commitTransaction(ctx context.Context,
	t *StreamTask,
	tm *transaction.TransactionManager,
	args *StreamTaskArgs,
	hasLiveTransaction *bool,
	trackConsumePar *bool,
	paused *bool,
) *common.FnOutput {
	// debug.Fprintf(os.Stderr, "about to pause\n")
	if t.pauseFunc != nil {
		if ret := t.pauseFunc(); ret != nil {
			return ret
		}
		*paused = true
	}
	cBeg := stats.TimerBegin()
	offsetRecords := transaction.CollectOffsetRecords(args.ectx.Consumers())
	err := tm.AppendConsumedSeqNum(ctx, offsetRecords, args.ectx.SubstreamNum())
	if err != nil {
		fmt.Fprintf(os.Stderr, "[ERROR] append offset failed: %v\n", err)
		return &common.FnOutput{Success: false, Message: fmt.Sprintf("append offset failed: %v\n", err)}
	}
	err = tm.CommitTransaction(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[ERROR] commit failed: %v\n", err)
		return &common.FnOutput{Success: false, Message: fmt.Sprintf("commit failed: %v\n", err)}
	}
	cElapsed := stats.Elapsed(cBeg).Microseconds()
	t.commitTrTime.AddSample(cElapsed)
	*hasLiveTransaction = false
	*trackConsumePar = false
	return nil
}
