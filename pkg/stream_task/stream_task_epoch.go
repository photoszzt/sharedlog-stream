package stream_task

import (
	"context"
	"os"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/control_channel"
	"sharedlog-stream/pkg/debug"
	"sharedlog-stream/pkg/epoch_manager"
	"sharedlog-stream/pkg/exactly_once_intr"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/stats"
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
	recentMeta, metaSeqNum, err := em.Init(ctx)
	if err != nil {
		return nil, nil, err
	}
	if recentMeta != nil {
		debug.Fprint(os.Stderr, "start restore\n")
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
		err = configChangelogExactlyOnce(em, args)
		if err != nil {
			return nil, nil, err
		}
		err = restoreStateStore(ctx, args, offsetMap)
		if err != nil {
			return nil, nil, err
		}
		setOffsetOnStream(offsetMap, args)
		debug.Fprintf(os.Stderr, "down restore\n")
	}
	cmm, err := control_channel.NewControlChannelManager(args.env, args.appId,
		args.serdeFormat, args.ectx.CurEpoch())
	if err != nil {
		return nil, nil, err
	}
	trackParFunc := exactly_once_intr.TrackProdSubStreamFunc(
		func(ctx context.Context, key interface{}, keySerde commtypes.Encoder,
			topicName string, substreamId uint8,
		) error {
			_ = em.AddTopicSubstream(ctx, topicName, substreamId)
			return control_channel.TrackAndAppendKeyMapping(ctx, cmm, key, keySerde, substreamId, topicName)
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
			cmm.TrackStream(name, stream)
		})
	if err != nil {
		return common.GenErrFnOutput(err)
	}

	dctx, dcancel := context.WithCancel(ctx)
	em.StartMonitorLog(dctx, dcancel)
	cmm.StartMonitorControlChannel(dctx)

	run := false
	hasProcessData := false
	init := false
	paused := false
	latencies := stats.NewInt64Collector("latPerIter", stats.DEFAULT_COLLECT_DURATION)
	markTimer := time.Now()
	var once sync.Once
	warmupCheck := stats.NewWarmupChecker(args.warmup)
	debug.Fprintf(os.Stderr, "commit every(ms): %d\n", args.commitEvery)
	for {
		ret := checkMonitorReturns(dctx, dcancel, args, cmm, em, &run)
		if ret != nil {
			return ret
		}
		if run {
			procStart := time.Now()
			once.Do(func() {
				warmupCheck.StartWarmup()
			})
			warmupCheck.Check()
			timeSinceLastMark := time.Since(markTimer)
			cur_elapsed := warmupCheck.ElapsedSinceInitial()
			timeout := args.duration != 0 && cur_elapsed >= args.duration
			shouldMarkByTime := (args.commitEvery != 0 && timeSinceLastMark > args.commitEvery)
			if (shouldMarkByTime || timeout) && hasProcessData {
				err_out := markEpoch(dctx, em, cmm, t, args, &hasProcessData, &paused)
				if err_out != nil {
					return err_out
				}
			}
			// Exit routine
			cur_elapsed = warmupCheck.ElapsedSinceInitial()
			timeout = args.duration != 0 && cur_elapsed >= args.duration
			if timeout {
				elapsed := time.Since(procStart)
				latencies.AddSample(elapsed.Microseconds())
				ret := &common.FnOutput{Success: true}
				updateReturnMetric(ret, &warmupCheck)
				return ret
			}

			// init
			if !hasProcessData {
				err := initAfterMarkOrCommit(ctx, t, args, em, &init, &paused)
				if err != nil {
					return common.GenErrFnOutput(err)
				}
				markTimer = time.Now()
			}
			ret := t.appProcessFunc(ctx, t, args.ectx)
			if ret != nil {
				if ret.Success {
					// consume timeout but not sure whether there's more data is coming; continue to process
					continue
				}
				return ret
			}
			if !hasProcessData {
				hasProcessData = true
			}
			if warmupCheck.AfterWarmup() {
				elapsed := time.Since(procStart)
				latencies.AddSample(elapsed.Microseconds())
			}
		}
	}
}

func markEpoch(ctx context.Context,
	em *epoch_manager.EpochManager,
	cmm *control_channel.ControlChannelManager,
	t *StreamTask,
	args *StreamTaskArgs,
	hasProcessData *bool,
	paused *bool,
) *common.FnOutput {
	if t.pauseFunc != nil {
		if ret := t.pauseFunc(); ret != nil {
			return ret
		}
		*paused = true
	}
	err := cmm.FlushControlLog(ctx)
	if err != nil {
		return common.GenErrFnOutput(err)
	}
	mStart := stats.TimerBegin()
	epochMarker, err := epoch_manager.GenEpochMarker(ctx, em, args.ectx.Consumers(), args.ectx.Producers(),
		args.kvChangelogs, args.windowStoreChangelogs)
	if err != nil {
		return common.GenErrFnOutput(err)
	}
	err = epoch_manager.MarkEpochAndCleanupState(ctx, em, epochMarker, args.ectx.Producers(),
		args.kvChangelogs, args.windowStoreChangelogs)
	if err != nil {
		return common.GenErrFnOutput(err)
	}
	mElapsed := stats.Elapsed(mStart).Microseconds()
	t.markEpochTime.AddSample(mElapsed)
	*hasProcessData = false
	return nil
}
