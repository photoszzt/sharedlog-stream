package stream_task

import (
	"context"
	"os"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/pkg/common_errors"
	"sharedlog-stream/pkg/consume_seq_num_manager"
	"sharedlog-stream/pkg/consume_seq_num_manager/con_types"
	"sharedlog-stream/pkg/debug"
	"sharedlog-stream/pkg/stats"
	"time"
)

func (t *StreamTask) process(ctx context.Context, args *StreamTaskArgs) *common.FnOutput {
	debug.Assert(len(args.ectx.Consumers()) >= 1, "Srcs should be filled")
	debug.Assert(args.env != nil, "env should be filled")
	debug.Assert(args.ectx != nil, "program args should be filled")
	latencies := stats.NewInt64Collector("latPerIter", stats.DEFAULT_COLLECT_DURATION)
	cm, err := consume_seq_num_manager.NewConsumeSeqManager(args.serdeFormat)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	debug.Fprint(os.Stderr, "start restore\n")
	for _, src := range args.ectx.Consumers() {
		inputTopicName := src.TopicName()
		err = cm.CreateOffsetTopic(args.env, inputTopicName, src.Stream().NumPartition(), args.serdeFormat)
		if err != nil {
			return &common.FnOutput{Success: false, Message: err.Error()}
		}
		offset, err := cm.FindLastConsumedSeqNum(ctx, inputTopicName, args.ectx.SubstreamNum())
		if err != nil {
			if !common_errors.IsStreamEmptyError(err) {
				return &common.FnOutput{Success: false, Message: err.Error()}
			}
		}
		debug.Fprintf(os.Stderr, "offset restores to %x\n", offset)
		if offset != 0 {
			src.SetCursor(offset+1, args.ectx.SubstreamNum())
		}
		cm.AddTopicTrackConsumedSeqs(ctx, inputTopicName, []uint8{args.ectx.SubstreamNum()})
	}
	debug.Fprint(os.Stderr, "done restore\n")
	args.ectx.StartWarmup()
	if t.initFunc != nil {
		t.initFunc(t)
	}
	hasUntrackedConsume := false

	debug.Fprintf(os.Stderr, "warmup time: %v, flush every: %v\n", args.warmup, args.flushEvery)
	warmupCheck := stats.NewWarmupChecker(args.warmup)
	warmupCheck.StartWarmup()
	commitTimer := time.Now()
	flushTimer := time.Now()
	for {
		timeSinceLastTrack := time.Since(commitTimer)
		if timeSinceLastTrack >= args.trackEveryForAtLeastOnce {
			if ret_err := t.pauseTrackFlushResume(ctx, args, cm, &hasUntrackedConsume, &commitTimer, &flushTimer); ret_err != nil {
				return ret_err
			}
		}
		timeSinceLastFlush := time.Since(flushTimer)
		if timeSinceLastFlush >= args.flushEvery {
			if ret_err := t.pauseFlushResume(ctx, args, &flushTimer); err != nil {
				return ret_err
			}
		}
		warmupCheck.Check()
		if args.duration != 0 && warmupCheck.ElapsedSinceInitial() >= args.duration {
			break
		}
		procStart := time.Now()
		ret := t.appProcessFunc(ctx, t, args.ectx)
		if ret != nil {
			if ret.Success {
				continue
			}
			return ret
		}
		if !hasUntrackedConsume {
			hasUntrackedConsume = true
		}
		if warmupCheck.AfterWarmup() {
			elapsed := time.Since(procStart)
			latencies.AddSample(elapsed.Microseconds())
		}
	}
	t.pauseTrackFlush(ctx, args, cm, hasUntrackedConsume)
	ret := &common.FnOutput{Success: true}
	updateReturnMetric(ret, &warmupCheck)
	return ret
}

func (t *StreamTask) pauseFlushResume(ctx context.Context, args *StreamTaskArgs, flushTimer *time.Time) *common.FnOutput {
	if t.pauseFunc != nil {
		if ret := t.pauseFunc(args); ret != nil {
			return ret
		}
	}
	err := t.flushStreams(ctx, args)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	if t.resumeFunc != nil {
		t.resumeFunc(t, args)
	}
	*flushTimer = time.Now()
	return nil
}

func (t *StreamTask) pauseTrackFlushResume(ctx context.Context, args *StreamTaskArgs, cm *consume_seq_num_manager.ConsumeSeqManager,
	hasUncommitted *bool, commitTimer *time.Time, flushTimer *time.Time,
) *common.FnOutput {
	if t.pauseFunc != nil {
		if ret := t.pauseFunc(args); ret != nil {
			return ret
		}
	}
	err := t.trackSrcConSeq(ctx, cm, args.ectx.SubstreamNum())
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	err = t.flushStreams(ctx, args)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	if t.resumeFunc != nil {
		t.resumeFunc(t, args)
	}
	*hasUncommitted = false
	*commitTimer = time.Now()
	*flushTimer = time.Now()
	return nil
}

func (t *StreamTask) pauseTrackFlush(ctx context.Context, args *StreamTaskArgs, cm *consume_seq_num_manager.ConsumeSeqManager, hasUncommitted bool) *common.FnOutput {
	alreadyPaused := false
	if hasUncommitted {
		if t.pauseFunc != nil {
			if ret := t.pauseFunc(args); ret != nil {
				return ret
			}
		}
		err := t.trackSrcConSeq(ctx, cm, args.ectx.SubstreamNum())
		if err != nil {
			return &common.FnOutput{Success: false, Message: err.Error()}
		}
		alreadyPaused = true
	}
	if !alreadyPaused && t.pauseFunc != nil {
		if ret := t.pauseFunc(args); ret != nil {
			return ret
		}
	}
	err := t.flushStreams(ctx, args)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	return nil
}

func (t *StreamTask) trackSrcConSeq(ctx context.Context, cm *consume_seq_num_manager.ConsumeSeqManager, parNum uint8) error {
	consumedSeqNumConfigs := make([]con_types.ConsumedSeqNumConfig, 0)
	t.OffMu.Lock()
	for topic, offset := range t.CurrentConsumeOffset {
		consumedSeqNumConfigs = append(consumedSeqNumConfigs, con_types.ConsumedSeqNumConfig{
			TopicToTrack:   topic,
			Partition:      parNum,
			ConsumedSeqNum: uint64(offset),
		})
	}
	t.OffMu.Unlock()
	err := cm.AppendConsumedSeqNum(ctx, consumedSeqNumConfigs)
	if err != nil {
		return err
	}
	err = cm.Track(ctx)
	return err
}
