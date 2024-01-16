package stream_task

import (
	"context"
	"fmt"
	"os"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/pkg/checkpt"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/debug"
	"sharedlog-stream/pkg/snapshot_store"
	"sharedlog-stream/pkg/stats"
	"sharedlog-stream/pkg/store"
)

// when restoring the checkpoint, it's reading from
// the sequence number of the checkpoint marker.
// the source stream's record between the checkpoint marker
// and its current tail should be ignored.
//
// ┌───┬────┐  ┌───┐  ┌────┬────────┐  ┌───┐  ┌───┬────────┐
// │   │    ├──┤ A ├──┤    │disgard ├──┤ B ├──┤   │disgard │
// └───┴────┘  └───┘  └────┴────────┘  └───┘  └───┴────────┘
//  marker              marker                   marker

func SetKVStoreChkpt[K, V any](
	ctx context.Context,
	rs *snapshot_store.RedisSnapshotStore,
	kvstore store.CoreKeyValueStoreG[K, V],
	chkptSerde commtypes.SerdeG[commtypes.Checkpoint],
) {
	kvstore.SetSnapshotCallback(ctx,
		func(ctx context.Context,
			tpLogoff []commtypes.TpLogOff,
			chkptMeta []commtypes.ChkptMetaData,
			snapshot []commtypes.KeyValuePair[K, V],
		) error {
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
				KvArr:     outBin,
				ChkptMeta: chkptMeta,
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
	chkptSerde commtypes.SerdeG[commtypes.Checkpoint],
) {
	winStore.SetWinSnapshotCallback(ctx,
		func(ctx context.Context,
			tpLogOff []commtypes.TpLogOff,
			chkptMeta []commtypes.ChkptMetaData,
			snapshot []commtypes.KeyValuePair[commtypes.KeyAndWindowStartTsG[K], V],
		) error {
			kvPairSerdeG := winStore.GetKVSerde()
			outBin := make([][]byte, 0, len(snapshot))
			for _, kv := range snapshot {
				bin, err := kvPairSerdeG.Encode(kv)
				if err != nil {
					return err
				}
				outBin = append(outBin, bin)
			}
			out, err := chkptSerde.Encode(commtypes.Checkpoint{
				KvArr:     outBin,
				ChkptMeta: chkptMeta,
			})
			if err != nil {
				return err
			}
			fmt.Fprintf(os.Stderr, "win snapshot size: %d, store snapshot at %x\n", len(out), tpLogOff[0].LogOff)
			return rs.StoreAlignChkpt(ctx, out, tpLogOff)
		})
}

func processAlignChkpt(ctx context.Context, t *StreamTask, args *StreamTaskArgs) *common.FnOutput {
	var finalOutTpNames []string
	debug.Assert(len(args.ectx.Consumers()) >= 1, "Srcs should be filled")
	debug.Assert(args.env != nil, "env should be filled")
	debug.Assert(args.ectx != nil, "program args should be filled")
	args.ectx.StartWarmup()
	if t.initFunc != nil {
		t.initFunc(t)
	}
	rcm, err := checkpt.NewRedisChkptManager(ctx)
	if err != nil {
		return common.GenErrFnOutput(err)
	}
	debug.Fprintf(os.Stderr, "warmup time: %v, flush every: %v, waitEndMark: %v\n",
		args.warmup, args.flushEvery, args.waitEndMark)
	warmupCheck := stats.NewWarmupChecker(args.warmup)
	warmupCheck.StartWarmup()
	for _, tp := range args.ectx.Producers() {
		finalOutTpNames = append(finalOutTpNames, tp.TopicName())
	}
	for {
		warmupCheck.Check()
		ret, ctrlRawMsgArr := t.appProcessFunc(ctx, t, args.ectx)
		if ret != nil {
			if ret.Success {
				continue
			}
			return ret
		}
		if ctrlRawMsgArr != nil {
			fmt.Fprintf(os.Stderr, "exit due to ctrlMsg\n")
			if ret_err := pauseTimedFlushStreams(ctx, t, args); ret_err != nil {
				return ret_err
			}
			if ctrlRawMsgArr[0].Mark != commtypes.CHKPT_MARK {
				return handleCtrlMsg(ctx, ctrlRawMsgArr, t, args, &warmupCheck)
			}
			err := checkpoint(ctx, t, args, ctrlRawMsgArr, &rcm, finalOutTpNames)
			if err != nil {
				return common.GenErrFnOutput(err)
			}
		}
	}
}

func checkpoint(
	ctx context.Context,
	t *StreamTask,
	args *StreamTaskArgs,
	ctrlRawMsgArr []*commtypes.RawMsgAndSeq,
	rcm *checkpt.RedisChkptManager,
	finalOutTpNames []string,
) error {
	var tpLogOff []commtypes.TpLogOff
	var chkptMeta []commtypes.ChkptMetaData
	for idx, c := range args.ectx.Consumers() {
		tlo := commtypes.TpLogOff{
			Tp:     c.Stream().TopicName(),
			LogOff: ctrlRawMsgArr[idx].FirstChkptMarkSeq,
		}
		tpLogOff = append(tpLogOff, tlo)
		chkptMeta = append(chkptMeta, commtypes.ChkptMetaData{
			Unprocessed:     ctrlRawMsgArr[idx].UnprocessSeq,
			LastChkptMarker: ctrlRawMsgArr[idx].LogSeqNum,
		})
	}
	epochMarker := commtypes.EpochMarker{
		StartTime: ctrlRawMsgArr[0].StartTime,
		Mark:      commtypes.CHKPT_MARK,
		ProdIndex: args.ectx.SubstreamNum(),
	}
	encoded, err := args.epochMarkerSerde.Encode(epochMarker)
	if err != nil {
		return err
	}
	ctrlRawMsgArr[0].Payload = encoded
	err = forwardCtrlMsg(ctx, ctrlRawMsgArr[0], args, "chkpt mark")
	if err != nil {
		return err
	}
	createChkpt(args, tpLogOff, chkptMeta)
	if t.isFinalStage {
		err = rcm.FinishChkpt(ctx, finalOutTpNames)
		if err != nil {
			return err
		}
	}
	return nil
}
