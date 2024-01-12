package stream_task

import (
	"context"
	"fmt"
	"os"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/debug"
	"sharedlog-stream/pkg/snapshot_store"
	"sharedlog-stream/pkg/stats"
	"sharedlog-stream/pkg/store"
)

func SetKVStoreChkpt[K, V any](
	ctx context.Context,
	rs *snapshot_store.RedisSnapshotStore,
	kvstore store.CoreKeyValueStoreG[K, V],
	chkptSerde commtypes.SerdeG[commtypes.Checkpoint],
) {
	kvstore.SetSnapshotCallback(ctx,
		func(ctx context.Context, tpLogoff []commtypes.TpLogOff, unprocessed [][]uint64, snapshot []commtypes.KeyValuePair[K, V]) error {
			kvPairSerdeG := kvstore.GetKVSerde()
			outBin := make([][]byte, 0, len(snapshot))
			for _, kv := range snapshot {
				bin, err := kvPairSerdeG.Encode(kv)
				if err != nil {
					return err
				}
				outBin = append(outBin, bin)
			}
			out, err := chkptSerde.Encode(commtypes.Checkpoint{
				Kvarr:       outBin,
				Unprocessed: unprocessed,
			})
			if err != nil {
				return err
			}
			fmt.Fprintf(os.Stderr, "kv snapshot size: %d, store snapshot at %x\n", len(out), tpLogoff[0].LogOff)
			return rs.StoreAlignChkpt(ctx, out, tpLogoff)
		})
}

func SetWinStoreChkpt[K, V any](
	ctx context.Context,
	rs *snapshot_store.RedisSnapshotStore,
	winStore store.CoreWindowStoreG[K, V],
	payloadSerde commtypes.SerdeG[commtypes.PayloadArr],
) {
	winStore.SetWinSnapshotCallback(ctx,
		func(ctx context.Context, tpLogOff []commtypes.TpLogOff,
			snapshot []commtypes.KeyValuePair[commtypes.KeyAndWindowStartTsG[K], V],
		) error {
			out, err := encodeWinSnapshot[K, V](winStore, snapshot, payloadSerde)
			if err != nil {
				return err
			}
			fmt.Fprintf(os.Stderr, "win snapshot size: %d, store snapshot at %x\n", len(out), tpLogOff[0].LogOff)
			return rs.StoreAlignChkpt(ctx, out, tpLogOff)
		})
}

func processAlignChkpt(ctx context.Context, t *StreamTask, args *StreamTaskArgs) *common.FnOutput {
	debug.Assert(len(args.ectx.Consumers()) >= 1, "Srcs should be filled")
	debug.Assert(args.env != nil, "env should be filled")
	debug.Assert(args.ectx != nil, "program args should be filled")
	args.ectx.StartWarmup()
	if t.initFunc != nil {
		t.initFunc(t)
	}

	debug.Fprintf(os.Stderr, "warmup time: %v, flush every: %v, waitEndMark: %v\n",
		args.warmup, args.flushEvery, args.waitEndMark)
	warmupCheck := stats.NewWarmupChecker(args.warmup)
	warmupCheck.StartWarmup()
	gotEndMark := false
	for {
		warmupCheck.Check()
		if (!args.waitEndMark && args.duration != 0 && warmupCheck.ElapsedSinceInitial() >= args.duration) || gotEndMark {
			break
		}
		ret, ctrlRawMsgArr := t.appProcessFunc(ctx, t, args.ectx)
		if ret != nil {
			if ret.Success {
				continue
			}
			return ret
		}
		if ctrlRawMsgArr != nil {
			fmt.Fprintf(os.Stderr, "exit due to ctrlMsg\n")
			if ctrlRawMsgArr[0].Mark != commtypes.CHKPT_MARK {
				if t.pauseFunc != nil {
					if ret := t.pauseFunc(); ret != nil {
						return ret
					}
				}
				if ret_err := timedFlushStreams(ctx, t, args); ret_err != nil {
					return ret_err
				}
				return handleCtrlMsg(ctx, ctrlRawMsgArr[0], t, args, &warmupCheck)
			}
			tpLogOff := make([]commtypes.TpLogOff, 0, len(ctrlRawMsgArr))
			unprocessed := make([][]uint64, 0, len(ctrlRawMsgArr))
			for idx, c := range args.ectx.Consumers() {
				tlo := commtypes.TpLogOff{
					Tp:     c.Stream().TopicName(),
					LogOff: ctrlRawMsgArr[idx].FirstChkptMarkSeq,
				}
				tpLogOff = append(tpLogOff, tlo)
				unprocessed = append(unprocessed, ctrlRawMsgArr[idx].UnprocessSeq)
			}
			createChkpt(args, tpLogOff, unprocessed)
		}
	}
	if t.pauseFunc != nil {
		if ret := t.pauseFunc(); ret != nil {
			return ret
		}
	}
	if ret_err := timedFlushStreams(ctx, t, args); ret_err != nil {
		return ret_err
	}
	ret := &common.FnOutput{Success: true}
	updateReturnMetric(ret, &warmupCheck,
		args.waitEndMark, t.GetEndDuration(), args.ectx.SubstreamNum())
	return ret
}
