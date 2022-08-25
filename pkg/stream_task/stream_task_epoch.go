package stream_task

import (
	"context"
	"fmt"
	"os"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/pkg/common_errors"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/control_channel"
	"sharedlog-stream/pkg/debug"
	"sharedlog-stream/pkg/epoch_manager"
	"sharedlog-stream/pkg/exactly_once_intr"
	"sharedlog-stream/pkg/producer_consumer"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/stats"
	"sharedlog-stream/pkg/store"
	"sync"
	"time"
)

func SetupManagersForEpoch(ctx context.Context,
	args *StreamTaskArgs,
) (*epoch_manager.EpochManager, *control_channel.ControlChannelManager, error) {
	em, err := epoch_manager.NewEpochManager(args.env, args.transactionalId, args.serdeFormat)
	if err != nil {
		return nil, nil, err
	}
	initEmStart := time.Now()
	recentMeta, metaSeqNum, err := em.Init(ctx)
	if err != nil {
		return nil, nil, err
	}
	initEmElapsed := time.Since(initEmStart)
	fmt.Fprintf(os.Stderr, "Init EpochManager took %v\n", initEmElapsed)
	err = configChangelogExactlyOnce(em, args)
	if err != nil {
		return nil, nil, err
	}
	if recentMeta != nil {
		fmt.Fprint(os.Stderr, "start restore\n")
		restoreBeg := time.Now()
		offsetMap := recentMeta.ConSeqNums
		if len(offsetMap) == 0 {
			// the last commit doesn't consume anything
			meta, err := em.FindMostRecentEpochMetaThatHasConsumed(ctx, metaSeqNum)
			if err != nil {
				return nil, nil, err
			}
			if meta == nil {
				panic("consumed sequnce number must be recorded in epoch meta")
			} else {
				offsetMap = meta.ConSeqNums
			}
		}
		err = restoreStateStore(ctx, args, offsetMap)
		if err != nil {
			return nil, nil, err
		}
		setOffsetOnStream(offsetMap, args)
		restoreElapsed := time.Since(restoreBeg)
		fmt.Fprintf(os.Stderr, "down restore, elapsed: %v\n", restoreElapsed)
	}
	cmm, err := control_channel.NewControlChannelManager(args.env, args.appId,
		args.serdeFormat, args.ectx.CurEpoch(), args.ectx.SubstreamNum())
	if err != nil {
		return nil, nil, err
	}
	trackParFunc := exactly_once_intr.TrackProdSubStreamFunc(
		func(ctx context.Context, kBytes []byte,
			topicName string, substreamId uint8,
		) error {
			_ = em.AddTopicSubstream(ctx, topicName, substreamId)
			return control_channel.TrackAndAppendKeyMapping(ctx, cmm, kBytes, substreamId, topicName)
		})
	recordFinish := func(ctx context.Context, funcName string, instanceID uint8) error {
		return cmm.RecordPrevInstanceFinish(ctx, funcName, instanceID, cmm.CurrentEpoch())
	}
	updateFuncs(args, trackParFunc, recordFinish)
	return em, cmm, nil
}

func processInEpoch(
	ctx context.Context,
	t *StreamTask,
	em *epoch_manager.EpochManager,
	cmm *control_channel.ControlChannelManager,
	args *StreamTaskArgs,
) *common.FnOutput {
	err := trackStreamAndConfigureExactlyOnce(args, em,
		func(name string, stream *sharedlog_stream.ShardedSharedLogStream) {
		})
	if err != nil {
		return common.GenErrFnOutput(err)
	}

	dctx, dcancel := context.WithCancel(ctx)
	em.StartMonitorLog(dctx, dcancel)
	err = cmm.RestoreMappingAndWaitForPrevTask(
		dctx, args.ectx.FuncName(), args.ectx.CurEpoch(), args.kvChangelogs, args.windowStoreChangelogs)
	if err != nil {
		return common.GenErrFnOutput(err)
	}
	// run := false
	hasProcessData := false
	init := false
	paused := false
	// latencies := stats.NewInt64Collector("latPerIter", stats.DEFAULT_COLLECT_DURATION)
	markTimer := time.Now()
	var once sync.Once
	warmupCheck := stats.NewWarmupChecker(args.warmup)
	debug.Fprintf(os.Stderr, "commit every(ms): %v, waitEndMark: %v, fixed output parNum: %d\n",
		args.commitEvery, args.waitEndMark, args.fixedOutParNum)
	testForFail := false
	var failAfter time.Duration
	failParam, ok := args.testParams[args.ectx.FuncName()]
	if ok && failParam.InstanceId == args.ectx.SubstreamNum() {
		testForFail = true
		failAfter = time.Duration(failParam.FailAfterS) * time.Second
		fmt.Fprintf(os.Stderr, "%s(%d) will fail after %v\n",
			args.ectx.FuncName(), args.ectx.SubstreamNum(), failAfter)
	}
	for {
		ret := checkMonitorReturns(dctx, dcancel, args, cmm, em)
		if ret != nil {
			return ret
		}
		// procStart := time.Now()
		once.Do(func() {
			warmupCheck.StartWarmup()
		})
		warmupCheck.Check()
		timeSinceLastMark := time.Since(markTimer)
		cur_elapsed := warmupCheck.ElapsedSinceInitial()
		timeout := args.duration != 0 && cur_elapsed >= args.duration
		shouldMarkByTime := (args.commitEvery != 0 && timeSinceLastMark > args.commitEvery)
		if (shouldMarkByTime || timeout) && hasProcessData {
			if t.pauseFunc != nil {
				if ret := t.pauseFunc(); ret != nil {
					return ret
				}
				paused = true
			}
			flushAllStart := stats.TimerBegin()
			err := FlushStreamBuffers(dctx, cmm, args)
			if err != nil {
				return common.GenErrFnOutput(err)
			}
			flushTime := stats.Elapsed(flushAllStart).Microseconds()
			err = markEpoch(dctx, em, t, args)
			if err != nil {
				return common.GenErrFnOutput(err)
			}
			t.flushAllTime.AddSample(flushTime)
			hasProcessData = false
		}
		// Exit routine
		cur_elapsed = warmupCheck.ElapsedSinceInitial()
		timeout = args.duration != 0 && cur_elapsed >= args.duration
		if (!args.waitEndMark && timeout) || (testForFail && cur_elapsed >= failAfter) {
			fmt.Fprintf(os.Stderr, "exit due to timeout\n")
			// elapsed := time.Since(procStart)
			// latencies.AddSample(elapsed.Microseconds())
			ret := &common.FnOutput{Success: true}
			if testForFail {
				ret = &common.FnOutput{Success: true, Message: common_errors.ErrReturnDueToTest.Error()}
			}
			updateReturnMetric(ret, &warmupCheck,
				false, t.GetEndDuration(), args.ectx.SubstreamNum())
			return ret
		}

		// init
		if !hasProcessData {
			err := initAfterMarkOrCommit(dctx, t, args, em, &init, &paused)
			if err != nil {
				return common.GenErrFnOutput(err)
			}
			markTimer = time.Now()
		}
		app_ret, ctrlMsg := t.appProcessFunc(dctx, t, args.ectx)
		if app_ret != nil {
			if app_ret.Success {
				debug.Assert(ctrlMsg == nil, "when timeout, ctrlMsg should not be returned")
				// consume timeout but not sure whether there's more data is coming; continue to process
				continue
			}
			return ret
		}
		if !hasProcessData {
			hasProcessData = true
		}
		if ctrlMsg != nil {
			// fmt.Fprintf(os.Stderr, "exit due to ctrlMsg\n")
			if t.pauseFunc != nil {
				if ret := t.pauseFunc(); ret != nil {
					return ret
				}
				paused = true
			}
			flushAllStart := stats.TimerBegin()
			err := FlushStreamBuffers(dctx, cmm, args)
			if err != nil {
				return common.GenErrFnOutput(err)
			}
			flushTime := stats.Elapsed(flushAllStart).Microseconds()
			err = markEpoch(dctx, em, t, args)
			if err != nil {
				return common.GenErrFnOutput(err)
			}
			t.flushAllTime.AddSample(flushTime)
			return handleCtrlMsg(dctx, ctrlMsg, t, args, &warmupCheck)
		}
		// if warmupCheck.AfterWarmup() {
		// 	elapsed := time.Since(procStart)
		// 	latencies.AddSample(elapsed.Microseconds())
		// }
	}
}

func markEpoch(ctx context.Context, em *epoch_manager.EpochManager,
	t *StreamTask, args *StreamTaskArgs,
) error {
	prepareStart := stats.TimerBegin()
	epochMarker, epochMarkerTags, epochMarkerTopics, err := CaptureEpochStateAndCleanup(ctx, em, args)
	if err != nil {
		return err
	}
	prepareTime := stats.Elapsed(prepareStart).Microseconds()
	mStart := stats.TimerBegin()
	err = em.MarkEpoch(ctx, epochMarker, epochMarkerTags, epochMarkerTopics)
	if err != nil {
		return err
	}
	mElapsed := stats.Elapsed(mStart).Microseconds()
	t.markEpochTime.AddSample(mElapsed)
	t.markEpochPrepare.AddSample(prepareTime)
	return nil
}

func FlushStreamBuffers(ctx context.Context, cmm *control_channel.ControlChannelManager, args *StreamTaskArgs) error {
	for _, cachedProcessor := range args.cachedProcessors {
		err := cachedProcessor.Flush(ctx)
		if err != nil {
			return err
		}
	}
	for _, kvTab := range args.kvChangelogs {
		if !kvTab.ChangelogIsSrc() {
			err := kvTab.Flush(ctx)
			if err != nil {
				return err
			}
		}
	}
	for _, winTab := range args.windowStoreChangelogs {
		err := winTab.Flush(ctx)
		if err != nil {
			return err
		}
	}
	for _, producer := range args.ectx.Producers() {
		err := producer.Flush(ctx)
		if err != nil {
			return err
		}
	}
	err := cmm.FlushControlLog(ctx)
	if err != nil {
		return err
	}
	return nil
}

func CaptureEpochStateAndCleanup(ctx context.Context, em *epoch_manager.EpochManager, args *StreamTaskArgs) (commtypes.EpochMarker, []uint64, []string, error) {
	epochMarker, err := epoch_manager.GenEpochMarker(ctx, em, args.ectx.Consumers(), args.ectx.Producers(),
		args.kvChangelogs, args.windowStoreChangelogs)
	if err != nil {
		return commtypes.EpochMarker{}, nil, nil, err
	}
	epochMarkTags, epochMarkTopics := em.GenTagsAndTopicsForEpochMarker()
	epoch_manager.CleanupState(em, args.ectx.Producers(), args.kvChangelogs, args.windowStoreChangelogs)
	return epochMarker, epochMarkTags, epochMarkTopics, nil
}

func CaptureEpochStateAndCleanupExplicit(ctx context.Context, em *epoch_manager.EpochManager,
	consumers []producer_consumer.MeteredConsumerIntr, producers []producer_consumer.MeteredProducerIntr,
	kvChangelogs map[string]store.KeyValueStoreOpWithChangelog,
	windowStoreChangelogs map[string]store.WindowStoreOpWithChangelog,
) (commtypes.EpochMarker, []uint64, []string, error) {
	epochMarker, err := epoch_manager.GenEpochMarker(ctx, em, consumers, producers,
		kvChangelogs, windowStoreChangelogs)
	if err != nil {
		return commtypes.EpochMarker{}, nil, nil, err
	}
	epochMarkTags, epochMarkTopics := em.GenTagsAndTopicsForEpochMarker()
	epoch_manager.CleanupState(em, producers, kvChangelogs, windowStoreChangelogs)
	return epochMarker, epochMarkTags, epochMarkTopics, nil
}
