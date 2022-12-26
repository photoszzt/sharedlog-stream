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
	"sharedlog-stream/pkg/snapshot_store"
	"sharedlog-stream/pkg/stats"
	"sharedlog-stream/pkg/transaction"
	"sync"
	"time"
)

var (
	ASYNC_SECOND_PHASE = checkAsyncSecondPhase()
)

func checkAsyncSecondPhase() bool {
	async2ndPhaseStr := os.Getenv("ASYNC_SECOND_PHASE")
	async2ndPhase := async2ndPhaseStr == "true" || async2ndPhaseStr == "1"
	fmt.Fprintf(os.Stderr, "env str: %s, async2ndPhase: %v\n", async2ndPhaseStr, async2ndPhase)
	return async2ndPhase
}

func setupManagersFor2pc(ctx context.Context, t *StreamTask,
	streamTaskArgs *StreamTaskArgs, rs *snapshot_store.RedisSnapshotStore,
	setupSnapshotCallback SetupSnapshotCallbackFunc,
) (*transaction.TransactionManager, *control_channel.ControlChannelManager, error) {
	debug.Fprint(os.Stderr, "setup transaction and control manager\n")
	tm, err := transaction.NewTransactionManager(ctx, streamTaskArgs.env,
		streamTaskArgs.transactionalId, streamTaskArgs.serdeFormat)
	if err != nil {
		return nil, nil, fmt.Errorf("NewTransactionManager failed: %v", err)
	}
	recentTxnMeta, recentCompleteTxn, err := tm.InitTransaction(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("InitTransaction failed: %v", err)
	}
	err = configChangelogExactlyOnce(tm, streamTaskArgs)
	if err != nil {
		return nil, nil, err
	}
	err = setupSnapshotCallback(ctx, streamTaskArgs.env,
		streamTaskArgs.serdeFormat, rs)
	if err != nil {
		return nil, nil, err
	}
	if recentTxnMeta != nil {
		debug.Fprint(os.Stderr, "start restore\n")
		restoreBeg := time.Now()
		offsetMap, err := getOffsetMap(ctx, tm, streamTaskArgs)
		if err != nil {
			return nil, nil, err
		}
		debug.Fprintf(os.Stderr, "got offset map: %v\n", offsetMap)
		auxData := recentCompleteTxn.AuxData
		auxMetaSeq := recentCompleteTxn.LogSeqNum
		if CREATE_SNAPSHOT {
			loadSnapBeg := time.Now()
			err = loadSnapshot(ctx, streamTaskArgs, auxData, auxMetaSeq, rs)
			if err != nil {
				return nil, nil, err
			}
			loadSnapElapsed := time.Since(loadSnapBeg)
			fmt.Fprintf(os.Stderr, "load snapshot took %v\n", loadSnapElapsed)
		}
		restoreSsBeg := time.Now()
		err = restoreStateStore(ctx, streamTaskArgs, offsetMap)
		if err != nil {
			return nil, nil, err
		}
		restoreStateStoreElapsed := time.Since(restoreSsBeg)
		fmt.Fprintf(os.Stderr, "restore state store took %v\n", restoreStateStoreElapsed)
		setOffsetOnStream(offsetMap, streamTaskArgs)
		debug.Fprintf(os.Stderr, "down restore\n")
		restoreElapsed := time.Since(restoreBeg)
		fmt.Fprintf(os.Stderr, "down restore, elapsed: %v\n", restoreElapsed)
	}

	cmm, err := control_channel.NewControlChannelManager(streamTaskArgs.env, streamTaskArgs.appId,
		commtypes.SerdeFormat(streamTaskArgs.serdeFormat), streamTaskArgs.ectx.CurEpoch(),
		streamTaskArgs.ectx.SubstreamNum())
	if err != nil {
		return nil, nil, err
	}
	trackParFunc := func(ctx context.Context, topicName string, substreamId uint8) error {
		return tm.AddTopicSubstream(ctx, topicName, substreamId)
	}
	recordFinish := func(ctx context.Context, funcName string, instanceID uint8) error {
		return cmm.RecordPrevInstanceFinish(ctx, funcName, instanceID, streamTaskArgs.ectx.CurEpoch())
	}
	updateFuncs(streamTaskArgs, trackParFunc, recordFinish)
	return tm, cmm, nil
}

func processWithTransaction(
	ctx context.Context,
	t *StreamTask,
	tm *transaction.TransactionManager,
	cmm *control_channel.ControlChannelManager,
	args *StreamTaskArgs,
	rs *snapshot_store.RedisSnapshotStore,
) *common.FnOutput {
	err := trackStreamAndConfigureExactlyOnce(args, tm,
		func(name string, stream *sharedlog_stream.ShardedSharedLogStream) {
			tm.RecordTopicStreams(name, stream)
		})
	if err != nil {
		return common.GenErrFnOutput(err)
	}

	debug.Fprintf(os.Stderr, "start restore mapping")
	err = cmm.RestoreMappingAndWaitForPrevTask(ctx, args.ectx.FuncName(),
		CREATE_SNAPSHOT, args.serdeFormat,
		args.kvChangelogs, args.windowStoreChangelogs, rs)
	if err != nil {
		return common.GenErrFnOutput(err)
	}
	// latencies := stats.NewStatsCollector[int64]("latPerIter", stats.DEFAULT_COLLECT_DURATION)
	hasLiveTransaction := false
	paused := false
	commitTimer := time.Now()

	debug.Fprintf(os.Stderr, "commit every: %v, guarantee: %v\n", args.commitEvery, args.guarantee)
	init := false
	var once sync.Once
	warmupCheck := stats.NewWarmupChecker(args.warmup)
	gotEndMark := false
	for {
		// procStart := time.Now()
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
		if (shouldCommitByTime || timeout || gotEndMark) && hasLiveTransaction {
			err_out := commitTransaction(ctx, t, tm, cmm, args, &hasLiveTransaction, &paused)
			if err_out != nil {
				return err_out
			}
			debug.Assert(!hasLiveTransaction, "after commit. there should be no live transaction\n")
			// debug.Fprintf(os.Stderr, "transaction committed\n")
		}

		// Exit routine
		cur_elapsed = warmupCheck.ElapsedSinceInitial()
		if (!args.waitEndMark && args.duration != 0 && cur_elapsed >= args.duration) || gotEndMark {
			// elapsed := time.Since(procStart)
			// latencies.AddSample(elapsed.Microseconds())
			ret := &common.FnOutput{Success: true}
			updateReturnMetric(ret, &warmupCheck,
				args.waitEndMark, t.GetEndDuration(), args.ectx.SubstreamNum())
			return ret
		}

		// begin new transaction
		err := startNewTransaction(ctx, t, args, tm, &hasLiveTransaction,
			&init, &paused, &commitTimer)
		if err != nil {
			return &common.FnOutput{Success: false, Message: err.Error()}
		}
		debug.Assert(hasLiveTransaction, "after start new transaction. there should be a live transaction\n")

		app_ret, ctrlRawMsgOp := t.appProcessFunc(ctx, t, args.ectx)
		if app_ret != nil {
			if app_ret.Success {
				// consume timeout but not sure whether there's more data is coming; continue to process
				debug.Assert(ctrlRawMsgOp.IsNone(), "when timeout, ctrlMsg should not be returned")
				continue
			} else {
				if hasLiveTransaction {
					if err := tm.AbortTransaction(ctx); err != nil {
						return &common.FnOutput{Success: false, Message: fmt.Sprintf("abort failed: %v\n", err)}
					}
				}
				return app_ret
			}
		}
		ctrlRawMsg, ok := ctrlRawMsgOp.Take()
		if ok {
			err_out := commitTransaction(ctx, t, tm, cmm, args, &hasLiveTransaction,
				&paused)
			if err_out != nil {
				return err_out
			}
		}
		return handleCtrlMsg(ctx, ctrlRawMsg, t, args, &warmupCheck)
		// if warmupCheck.AfterWarmup() {
		// 	elapsed := time.Since(procStart)
		// 	latencies.AddSample(elapsed.Microseconds())
		// }
	}
}

func startNewTransaction(ctx context.Context, t *StreamTask,
	args *StreamTaskArgs, tm *transaction.TransactionManager,
	hasLiveTransaction *bool, init *bool, paused *bool, commitTimer *time.Time,
) error {
	if !*hasLiveTransaction {
		if err := tm.BeginTransaction(ctx); err != nil {
			debug.Fprintf(os.Stderr, "[ERROR] transaction begin failed: %v", err)
			return fmt.Errorf("transaction begin failed: %v\n", err)
		}
		*hasLiveTransaction = true
		*commitTimer = time.Now()
		debug.Fprintf(os.Stderr, "fixedOutParNum: %d\n", args.fixedOutParNum)
		err := initAfterMarkOrCommit(ctx, t, args, tm, init, paused)
		if err != nil {
			return err
		}
	}
	return nil
}

func commitTransaction(ctx context.Context,
	t *StreamTask,
	tm *transaction.TransactionManager,
	cmm *control_channel.ControlChannelManager,
	args *StreamTaskArgs,
	hasLiveTransaction *bool,
	paused *bool,
) *common.FnOutput {
	// debug.Fprintf(os.Stderr, "about to pause\n")
	commitTxnBeg := stats.TimerBegin()
	if t.pauseFunc != nil {
		if ret := t.pauseFunc(); ret != nil {
			return ret
		}
		*paused = true
	}
	flushAllStart := stats.TimerBegin()
	f, err := flushStreams(ctx, args)
	if err != nil {
		return common.GenErrFnOutput(fmt.Errorf("flushStreams: %v", err))
	}
	flushTime := stats.Elapsed(flushAllStart).Microseconds()

	offsetBeg := stats.TimerBegin()
	for _, src := range args.ectx.Consumers() {
		if err := tm.AddTopicTrackConsumedSeqs(ctx, src.TopicName(), args.ectx.SubstreamNum()); err != nil {
			return common.GenErrFnOutput(err)
		}
	}
	offsetRecords := transaction.CollectOffsetRecords(args.ectx.Consumers())
	err = tm.AppendConsumedSeqNum(ctx, offsetRecords, args.ectx.SubstreamNum())
	if err != nil {
		fmt.Fprintf(os.Stderr, "[ERROR] append offset failed: %v\n", err)
		return common.GenErrFnOutput(fmt.Errorf("append offset failed: %v\n", err))
	}
	sendOffsetElapsed := stats.Elapsed(offsetBeg).Microseconds()

	cBeg := stats.TimerBegin()
	if ASYNC_SECOND_PHASE {
		err = tm.CommitTransactionAsyncComplete(ctx)
		if err != nil {
			fmt.Fprintf(os.Stderr, "[ERROR] commitAsyncComplete failed: %v\n", err)
			return common.GenErrFnOutput(fmt.Errorf("commitAsyncComplete failed: %v\n", err))
		}
	} else {
		err = tm.CommitTransaction(ctx)
		if err != nil {
			fmt.Fprintf(os.Stderr, "[ERROR] commit failed: %v\n", err)
			return common.GenErrFnOutput(fmt.Errorf("commit failed: %v\n", err))
		}
	}
	cElapsed := stats.Elapsed(cBeg).Microseconds()
	commitTxnElapsed := stats.Elapsed(commitTxnBeg).Microseconds()
	t.txnCommitTime.AddSample(commitTxnElapsed)
	t.commitTxnAPITime.AddSample(cElapsed)
	t.flushStageTime.AddSample(flushTime)
	t.sendOffsetTime.AddSample(sendOffsetElapsed)
	if f > 0 {
		t.flushAtLeastOne.AddSample(flushTime)
	}
	*hasLiveTransaction = false
	return nil
}
