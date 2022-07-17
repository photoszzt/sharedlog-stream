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

func (t *StreamTask) SetupManagersForEpoch(ctx context.Context,
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
		func(ctx context.Context, key interface{}, keySerde commtypes.Serde,
			topicName string, substreamId uint8,
		) error {
			_ = em.AddTopicSubstream(ctx, topicName, substreamId)
			return cmm.TrackAndAppendKeyMapping(ctx, key, keySerde, substreamId, topicName)
		})
	recordFinish := func(ctx context.Context, funcName string, instanceID uint8) error {
		return cmm.RecordPrevInstanceFinish(ctx, funcName, instanceID, cmm.CurrentEpoch())
	}
	updateFuncs(args, trackParFunc, recordFinish)
	return em, cmm, nil
}

func (t *StreamTask) processInEpoch(
	ctx context.Context,
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
	markEpochTime := stats.NewInt64Collector("markEpochTime", stats.DEFAULT_COLLECT_DURATION)
	reInitTime := stats.NewInt64Collector("reInitTime", stats.DEFAULT_COLLECT_DURATION)
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
				mStart := stats.TimerBegin()
				err_out := t.markEpoch(dctx, em, args, &hasProcessData, &paused)
				if err_out != nil {
					return err_out
				}
				elapsed := stats.Elapsed(mStart).Microseconds()
				markEpochTime.AddSample(elapsed)
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
				rStart := stats.TimerBegin()
				err := t.initAfterMarkOrCommit(ctx, args, em, &init, &paused)
				if err != nil {
					return common.GenErrFnOutput(err)
				}
				markTimer = time.Now()
				elapsed := stats.Elapsed(rStart).Microseconds()
				reInitTime.AddSample(elapsed)
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

func (t *StreamTask) markEpoch(ctx context.Context,
	em *epoch_manager.EpochManager,
	args *StreamTaskArgs,
	hasProcessData *bool,
	paused *bool,
) *common.FnOutput {
	if t.pauseFunc != nil {
		if ret := t.pauseFunc(args); ret != nil {
			return ret
		}
		*paused = true
	}
	err := em.MarkEpoch(ctx, args.ectx.Consumers(), args.ectx.Producers())
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	*hasProcessData = false
	return nil
}
