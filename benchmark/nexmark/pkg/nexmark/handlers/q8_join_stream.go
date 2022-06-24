package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/benchmark/common/benchutil"
	ntypes "sharedlog-stream/benchmark/nexmark/pkg/nexmark/types"
	nutils "sharedlog-stream/benchmark/nexmark/pkg/nexmark/utils"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/concurrent_skiplist"
	"sharedlog-stream/pkg/debug"
	"sharedlog-stream/pkg/execution"
	"sharedlog-stream/pkg/proc_interface"
	"sharedlog-stream/pkg/processor"
	"sharedlog-stream/pkg/producer_consumer"
	"sharedlog-stream/pkg/store"
	"sharedlog-stream/pkg/store_with_changelog"
	"sharedlog-stream/pkg/stream_task"
	"sharedlog-stream/pkg/utils"
	"time"

	"cs.utexas.edu/zjia/faas/types"
)

type q8JoinStreamHandler struct {
	env      types.Environment
	funcName string
}

func NewQ8JoinStreamHandler(env types.Environment, funcName string) types.FuncHandler {
	return &q8JoinStreamHandler{
		env:      env,
		funcName: funcName,
	}
}

func (h *q8JoinStreamHandler) Call(ctx context.Context, input []byte) ([]byte, error) {
	parsedInput := &common.QueryInput{}
	err := json.Unmarshal(input, parsedInput)
	if err != nil {
		return nil, err
	}
	output := h.Query8JoinStream(ctx, parsedInput)
	encodedOutput, err := json.Marshal(output)
	if err != nil {
		panic(err)
	}
	// fmt.Printf("query 3 output: %v\n", encodedOutput)
	return nutils.CompressData(encodedOutput), nil
}

func q8CompareFunc(lhs, rhs interface{}) int {
	l, ok := lhs.(uint64)
	if ok {
		r := rhs.(uint64)
		if l < r {
			return -1
		} else if l == r {
			return 0
		} else {
			return 1
		}
	} else {
		lv := lhs.(store.VersionedKey)
		rv := rhs.(store.VersionedKey)
		lvk := lv.Key.(uint64)
		rvk := rv.Key.(uint64)
		if lvk < rvk {
			return -1
		} else if lvk == rvk {
			if lv.Version < rv.Version {
				return -1
			} else if lv.Version == rv.Version {
				return 0
			} else {
				return 1
			}
		} else {
			return 1
		}
	}
}

func (h *q8JoinStreamHandler) getSrcSink(ctx context.Context, sp *common.QueryInput,
) ([]producer_consumer.MeteredConsumerIntr, []producer_consumer.MeteredProducerIntr, error) {
	stream1, stream2, outputStream, err := getInOutStreams(ctx, h.env, sp)
	if err != nil {
		return nil, nil, err
	}
	serdeFormat := commtypes.SerdeFormat(sp.SerdeFormat)
	msgSerde, err := commtypes.GetMsgSerde(serdeFormat)
	if err != nil {
		return nil, nil, fmt.Errorf("get msg serde err: %v", err)
	}

	eventSerde, err := ntypes.GetEventSerde(serdeFormat)
	if err != nil {
		return nil, nil, fmt.Errorf("get event serde err: %v", err)
	}
	timeout := common.SrcConsumeTimeout
	warmup := time.Duration(sp.WarmupS) * time.Second
	kvmsgSerdes := commtypes.KVMsgSerdes{
		KeySerde: commtypes.Uint64Serde{},
		ValSerde: eventSerde,
		MsgSerde: msgSerde,
	}
	ptSerde, err := ntypes.GetPersonTimeSerde(serdeFormat)
	if err != nil {
		return nil, nil, err
	}

	src1 := producer_consumer.NewMeteredConsumer(producer_consumer.NewShardedSharedLogStreamConsumer(stream1,
		&producer_consumer.StreamConsumerConfig{
			Timeout:     timeout,
			KVMsgSerdes: kvmsgSerdes,
		}), warmup)
	src2 := producer_consumer.NewMeteredConsumer(producer_consumer.NewShardedSharedLogStreamConsumer(stream2,
		&producer_consumer.StreamConsumerConfig{
			Timeout:     timeout,
			KVMsgSerdes: kvmsgSerdes,
		}), warmup)
	sink := producer_consumer.NewConcurrentMeteredSyncProducer(producer_consumer.NewShardedSharedLogStreamProducer(outputStream,
		&producer_consumer.StreamSinkConfig{
			KVMsgSerdes: commtypes.KVMsgSerdes{
				KeySerde: commtypes.Uint64Serde{},
				ValSerde: ptSerde,
				MsgSerde: msgSerde,
			},
			FlushDuration: time.Duration(sp.FlushMs) * time.Millisecond,
		}), warmup)
	src1.SetInitialSource(false)
	src2.SetInitialSource(false)
	sink.MarkFinalOutput()
	return []producer_consumer.MeteredConsumerIntr{src1, src2}, []producer_consumer.MeteredProducerIntr{sink}, nil
}

func getTabAndToTab(env types.Environment,
	sp *common.QueryInput,
	tabName string,
	compare concurrent_skiplist.CompareFunc,
	kvmegSerdes commtypes.KVMsgSerdes,
	joinWindows *processor.JoinWindows,
) (*processor.MeteredProcessor, store.WindowStore, *store_with_changelog.MaterializeParam, error) {
	format := commtypes.SerdeFormat(sp.SerdeFormat)
	mp, err := store_with_changelog.NewMaterializeParamBuilder().
		KVMsgSerdes(kvmegSerdes).
		StoreName(tabName).
		ParNum(sp.ParNum).
		SerdeFormat(format).
		StreamParam(commtypes.CreateStreamParam{
			Env:          env,
			NumPartition: sp.NumInPartition,
		}).BuildForKVStore(time.Duration(sp.FlushMs)*time.Millisecond, common.SrcConsumeTimeout)
	if err != nil {
		return nil, nil, nil, err
	}
	toTab, winTab := store_with_changelog.ToInMemWindowTableWithChangelog(mp, joinWindows, compare)
	return toTab, winTab, mp, nil
}

func (h *q8JoinStreamHandler) Query8JoinStream(ctx context.Context, sp *common.QueryInput) *common.FnOutput {
	debug.Assert(sp.ScaleEpoch != 0, "scale epoch should start from 1")
	srcs, sinks_arr, err := h.getSrcSink(ctx, sp)
	if err != nil {
		return &common.FnOutput{Success: false, Message: fmt.Sprintf("getSrcSink err: %v\n", err)}
	}
	joinWindows, err := processor.NewJoinWindowsWithGrace(time.Duration(10)*time.Second, time.Duration(5)*time.Second)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	compare := concurrent_skiplist.CompareFunc(q8CompareFunc)
	windowSizeMs := int64(10 * 1000)
	joiner := processor.ValueJoinerWithKeyTsFunc(func(readOnlyKey interface{},
		leftValue interface{}, rightValue interface{}, leftTs int64, rightTs int64) interface{} {
		// fmt.Fprint(os.Stderr, "get into joiner\n")
		rv := rightValue.(*ntypes.Event)
		ts := rv.NewPerson.DateTime
		windowStart := (utils.MaxInt64(0, ts-windowSizeMs+windowSizeMs) / windowSizeMs) * windowSizeMs
		return &ntypes.PersonTime{
			ID:        rv.NewPerson.ID,
			Name:      rv.NewPerson.Name,
			StartTime: windowStart,
		}
	})
	format := commtypes.SerdeFormat(sp.SerdeFormat)
	flushDur := time.Duration(sp.FlushMs) * time.Millisecond
	aucMp, err := store_with_changelog.NewMaterializeParamBuilder().
		KVMsgSerdes(srcs[0].KVMsgSerdes()).
		StoreName("auctionsBySellerIDWinTab").
		ParNum(sp.ParNum).
		SerdeFormat(format).
		StreamParam(commtypes.CreateStreamParam{
			Env:          h.env,
			NumPartition: sp.NumInPartition,
		}).BuildForWindowStore(flushDur, common.SrcConsumeTimeout)
	perMp, err := store_with_changelog.NewMaterializeParamBuilder().
		KVMsgSerdes(srcs[1].KVMsgSerdes()).
		StoreName("personsByIDWinTab").
		ParNum(sp.ParNum).
		SerdeFormat(format).
		StreamParam(commtypes.CreateStreamParam{
			Env:          h.env,
			NumPartition: sp.NumInPartition,
		}).BuildForWindowStore(flushDur, common.SrcConsumeTimeout)

	aucJoinsPerFunc, perJoinsAucFunc, wsc, err := execution.SetupStreamStreamJoin(aucMp, perMp, compare, joiner, joinWindows)
	if err != nil {
		return common.GenErrFnOutput(err)
	}
	task, procArgs := execution.PrepareTaskWithJoin(ctx,
		execution.JoinWorkerFunc(aucJoinsPerFunc),
		execution.JoinWorkerFunc(perJoinsAucFunc),
		proc_interface.NewBaseSrcsSinks(srcs, sinks_arr),
		proc_interface.NewBaseProcArgs(h.funcName, sp.ScaleEpoch, sp.ParNum),
	)
	streamTaskArgs := benchutil.UpdateStreamTaskArgs(sp,
		stream_task.NewStreamTaskArgsBuilder(h.env, procArgs, fmt.Sprintf("%s-%d", h.funcName, sp.ParNum))).
		WindowStoreChangelogs(wsc).FixedOutParNum(sp.ParNum).Build()
	return task.ExecuteApp(ctx, streamTaskArgs)
}
