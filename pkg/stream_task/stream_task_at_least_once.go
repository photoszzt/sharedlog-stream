package stream_task

import (
	"context"
	"os"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/pkg/common_errors"
	"sharedlog-stream/pkg/consume_seq_num_manager"
	"sharedlog-stream/pkg/debug"
	"sharedlog-stream/pkg/producer_consumer"
	"sharedlog-stream/pkg/stats"
	"time"
)

func process(ctx context.Context, t *StreamTask, args *StreamTaskArgs) *common.FnOutput {
	debug.Assert(len(args.ectx.Consumers()) >= 1, "Srcs should be filled")
	debug.Assert(args.env != nil, "env should be filled")
	debug.Assert(args.ectx != nil, "program args should be filled")
	// latencies := stats.NewInt64Collector("latPerIter", stats.DEFAULT_COLLECT_DURATION)
	cm, err := consume_seq_num_manager.NewConsumeSeqManager(args.serdeFormat)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	debug.Fprint(os.Stderr, "start restore\n")
	offsetMap := make(map[string]uint64)
	for _, src := range args.ectx.Consumers() {
		inputTopicName := src.TopicName()
		err = cm.CreateOffsetTopic(args.env, inputTopicName, src.Stream().NumPartition(), args.serdeFormat)
		if err != nil {
			return common.GenErrFnOutput(err)
		}
		offset, err := cm.FindLastConsumedSeqNum(ctx, inputTopicName, args.ectx.SubstreamNum())
		if err != nil {
			if !common_errors.IsStreamEmptyError(err) {
				return common.GenErrFnOutput(err)
			}
		}
		offsetMap[inputTopicName] = offset
		// debug.Fprintf(os.Stderr, "offset restores to %x\n", offset)
		// if offset != 0 {
		// 	src.SetCursor(offset+1, args.ectx.SubstreamNum())
		// }
	}
	err = restoreStateStore(ctx, args, offsetMap)
	if err != nil {
		return common.GenErrFnOutput(err)
	}
	setOffsetOnStream(offsetMap, args)
	debug.Fprint(os.Stderr, "done restore\n")
	args.ectx.StartWarmup()
	if t.initFunc != nil {
		t.initFunc(t)
	}
	hasUntrackedConsume := false

	debug.Fprintf(os.Stderr, "warmup time: %v, flush every: %v, waitEndMark: %v\n",
		args.warmup, args.flushEvery, args.waitEndMark)
	warmupCheck := stats.NewWarmupChecker(args.warmup)
	warmupCheck.StartWarmup()
	commitTimer := time.Now()
	flushTimer := time.Now()
	gotEndMark := false
	for {
		timeSinceLastTrack := time.Since(commitTimer)
		if timeSinceLastTrack >= args.trackEveryForAtLeastOnce {
			if ret_err := track(ctx, cm, args.ectx.Consumers(), args.ectx.SubstreamNum(), &hasUntrackedConsume, &commitTimer); ret_err != nil {
				return ret_err
			}
		}
		timeSinceLastFlush := time.Since(flushTimer)
		if timeSinceLastFlush >= args.flushEvery {
			if t.pauseFunc != nil {
				t.pauseFunc()
			}
			if ret_err := flushStreams(ctx, t, args); ret_err != nil {
				return common.GenErrFnOutput(ret_err)
			}
			if t.resumeFunc != nil {
				t.resumeFunc(t)
			}
			flushTimer = time.Now()
		}
		warmupCheck.Check()
		if (!args.waitEndMark && args.duration != 0 && warmupCheck.ElapsedSinceInitial() >= args.duration) || gotEndMark {
			break
		}
		// procStart := time.Now()
		ret := t.appProcessFunc(ctx, t, args.ectx, &gotEndMark)
		if ret != nil {
			if ret.Success {
				continue
			}
			return ret
		}
		if !hasUntrackedConsume {
			hasUntrackedConsume = true
		}
		// if warmupCheck.AfterWarmup() {
		// 	elapsed := time.Since(procStart)
		// 	latencies.AddSample(elapsed.Microseconds())
		// }
	}
	pauseTrackFlush(ctx, t, args, cm, hasUntrackedConsume)
	ret := &common.FnOutput{Success: true}
	updateReturnMetric(ret, &warmupCheck,
		args.waitEndMark, t.GetEndDuration(), args.ectx.SubstreamNum())
	return ret
}

func track(ctx context.Context, cm *consume_seq_num_manager.ConsumeSeqManager,
	consumers []producer_consumer.MeteredConsumerIntr, substreamNum uint8,
	hasUncommitted *bool, commitTimer *time.Time,
) *common.FnOutput {
	markers := consume_seq_num_manager.CollectOffsetMarker(consumers)
	err := cm.Track(ctx, markers, substreamNum)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	*hasUncommitted = false
	*commitTimer = time.Now()
	return nil
}

func pauseTrackFlush(ctx context.Context, t *StreamTask, args *StreamTaskArgs, cm *consume_seq_num_manager.ConsumeSeqManager, hasUncommitted bool) *common.FnOutput {
	alreadyPaused := false
	if hasUncommitted {
		if t.pauseFunc != nil {
			if ret := t.pauseFunc(); ret != nil {
				return ret
			}
		}
		markers := consume_seq_num_manager.CollectOffsetMarker(args.ectx.Consumers())
		err := cm.Track(ctx, markers, args.ectx.SubstreamNum())
		if err != nil {
			return &common.FnOutput{Success: false, Message: err.Error()}
		}
		alreadyPaused = true
	}
	if !alreadyPaused && t.pauseFunc != nil {
		if ret := t.pauseFunc(); ret != nil {
			return ret
		}
	}
	err := flushStreams(ctx, t, args)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	return nil
}
