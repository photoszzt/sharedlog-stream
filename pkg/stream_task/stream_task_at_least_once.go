package stream_task

import (
	"context"
	"os"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/pkg/common_errors"
	"sharedlog-stream/pkg/consume_seq_num_manager"
	"sharedlog-stream/pkg/consume_seq_num_manager/con_types"
	"sharedlog-stream/pkg/debug"
	"time"
)

func (t *StreamTask) Process(ctx context.Context, args *StreamTaskArgs) *common.FnOutput {
	debug.Assert(len(args.srcs) >= 1, "Srcs should be filled")
	debug.Assert(args.env != nil, "env should be filled")
	debug.Assert(args.procArgs != nil, "program args should be filled")
	debug.Assert(args.numInPartition != 0, "number of input partition should not be zero")
	latencies := make([]int, 0, 128)
	cm, err := consume_seq_num_manager.NewConsumeSeqManager(args.serdeFormat)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	debug.Fprint(os.Stderr, "start restore\n")
	for _, srcStream := range args.srcs {
		inputTopicName := srcStream.TopicName()
		err = cm.CreateOffsetTopic(args.env, inputTopicName, args.numInPartition, args.serdeFormat)
		if err != nil {
			return &common.FnOutput{Success: false, Message: err.Error()}
		}
		offset, err := cm.FindLastConsumedSeqNum(ctx, inputTopicName, args.parNum)
		if err != nil {
			if !common_errors.IsStreamEmptyError(err) {
				return &common.FnOutput{Success: false, Message: err.Error()}
			}
		}
		debug.Fprintf(os.Stderr, "offset restores to %x\n", offset)
		if offset != 0 {
			srcStream.SetCursor(offset+1, args.parNum)
		}
		cm.AddTopicTrackConsumedSeqs(ctx, inputTopicName, []uint8{args.parNum})
	}
	debug.Fprint(os.Stderr, "done restore\n")
	if t.initFunc != nil {
		t.initFunc(args.procArgs)
	}
	hasUntrackedConsume := false

	var afterWarmupStart time.Time
	afterWarmup := false
	commitTimer := time.Now()
	flushTimer := time.Now()
	debug.Fprintf(os.Stderr, "warmup time: %v\n", args.warmup)
	startTime := time.Now()
	for {
		timeSinceLastTrack := time.Since(commitTimer)
		if timeSinceLastTrack >= t.trackEveryForAtLeastOnce && t.CurrentOffset != nil {
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
		if !afterWarmup && args.warmup != 0 && time.Since(startTime) >= args.warmup {
			afterWarmup = true
			afterWarmupStart = time.Now()
		}
		if args.duration != 0 && time.Since(startTime) >= args.duration {
			break
		}
		procStart := time.Now()
		ret := t.appProcessFunc(ctx, t, args.procArgs)
		if ret != nil {
			if ret.Success {
				// elapsed := time.Since(procStart)
				// latencies = append(latencies, int(elapsed.Microseconds()))
				debug.Fprintf(os.Stderr, "consume timeout\n")
				if ret_err := t.pauseTrackFlush(ctx, args, cm, hasUntrackedConsume); ret_err != nil {
					return ret_err
				}
				updateReturnMetric(ret, startTime, afterWarmupStart, args.warmup, afterWarmup, latencies)
			}
			return ret
		}
		if !hasUntrackedConsume {
			hasUntrackedConsume = true
		}
		if args.warmup == 0 || afterWarmup {
			elapsed := time.Since(procStart)
			latencies = append(latencies, int(elapsed.Microseconds()))
		}
	}
	t.pauseTrackFlush(ctx, args, cm, hasUntrackedConsume)
	ret := &common.FnOutput{Success: true}
	updateReturnMetric(ret, startTime, afterWarmupStart, args.warmup, afterWarmup, latencies)
	return ret
}

func (t *StreamTask) pauseFlushResume(ctx context.Context, args *StreamTaskArgs, flushTimer *time.Time) *common.FnOutput {
	if t.pauseFunc != nil {
		if ret := t.pauseFunc(); ret != nil {
			return ret
		}
	}
	err := t.flushStreams(ctx, args)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	if t.resumeFunc != nil {
		t.resumeFunc(t)
	}
	*flushTimer = time.Now()
	return nil
}

func (t *StreamTask) pauseTrackFlushResume(ctx context.Context, args *StreamTaskArgs, cm *consume_seq_num_manager.ConsumeSeqManager,
	hasUncommitted *bool, commitTimer *time.Time, flushTimer *time.Time,
) *common.FnOutput {
	if t.pauseFunc != nil {
		if ret := t.pauseFunc(); ret != nil {
			return ret
		}
	}
	err := t.trackSrcConSeq(ctx, cm, args.parNum)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	err = t.flushStreams(ctx, args)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	if t.resumeFunc != nil {
		t.resumeFunc(t)
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
			if ret := t.pauseFunc(); ret != nil {
				return ret
			}
		}
		err := t.trackSrcConSeq(ctx, cm, args.parNum)
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
	err := t.flushStreams(ctx, args)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	return nil
}

func (t *StreamTask) trackSrcConSeq(ctx context.Context, cm *consume_seq_num_manager.ConsumeSeqManager, parNum uint8) error {
	consumedSeqNumConfigs := make([]con_types.ConsumedSeqNumConfig, 0)
	t.OffMu.Lock()
	for topic, offset := range t.CurrentOffset {
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
