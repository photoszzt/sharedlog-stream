package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/benchmark/common/benchutil"
	ntypes "sharedlog-stream/benchmark/nexmark/pkg/nexmark/types"
	"sharedlog-stream/benchmark/nexmark/pkg/nexmark/utils"
	"sharedlog-stream/pkg/errors"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/stream/processor"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"sharedlog-stream/pkg/stream/processor/store"
	"sharedlog-stream/pkg/treemap"
	"time"

	"cs.utexas.edu/zjia/faas/types"
	"golang.org/x/xerrors"
)

type q5MaxBid struct {
	env           types.Environment
	currentOffset map[string]uint64
}

func NewQ5MaxBid(env types.Environment) types.FuncHandler {
	return &q5MaxBid{
		env:           env,
		currentOffset: make(map[string]uint64),
	}
}

func (h *q5MaxBid) Call(ctx context.Context, input []byte) ([]byte, error) {
	parsedInput := &common.QueryInput{}
	err := json.Unmarshal(input, parsedInput)
	if err != nil {
		return nil, err
	}
	output := h.processQ5MaxBid(ctx, parsedInput)
	encodedOutput, err := json.Marshal(output)
	if err != nil {
		panic(err)
	}
	return utils.CompressData(encodedOutput), nil
}

func (h *q5MaxBid) getSrcSink(ctx context.Context, sp *common.QueryInput,
	input_stream *sharedlog_stream.ShardedSharedLogStream,
	output_stream *sharedlog_stream.ShardedSharedLogStream,
	seSerde commtypes.Serde, aucIdCountSerde commtypes.Serde,
	aucIdCountMaxSerde commtypes.Serde,
	msgSerde commtypes.MsgSerde,
) (*processor.MeteredSource, *processor.MeteredSink, error) {
	inConfig := &sharedlog_stream.SharedLogStreamConfig{
		Timeout:      common.SrcConsumeTimeout,
		KeyDecoder:   seSerde,
		ValueDecoder: aucIdCountSerde,
		MsgDecoder:   msgSerde,
	}
	outConfig := &sharedlog_stream.StreamSinkConfig{
		MsgSerde:   msgSerde,
		KeySerde:   seSerde,
		ValueSerde: aucIdCountMaxSerde,
	}
	src := processor.NewMeteredSource(sharedlog_stream.NewShardedSharedLogStreamSource(input_stream, inConfig))
	sink := processor.NewMeteredSink(sharedlog_stream.NewShardedSharedLogStreamSink(output_stream, outConfig))
	return src, sink, nil
}

type q5MaxBidProcessArgs struct {
	maxBid        *processor.MeteredProcessor
	stJoin        *processor.MeteredProcessor
	chooseMaxCnt  *processor.MeteredProcessor
	src           *processor.MeteredSource
	sink          *processor.MeteredSink
	output_stream *sharedlog_stream.ShardedSharedLogStream
	trackParFunc  sharedlog_stream.TrackKeySubStreamFunc
	parNum        uint8
}

func (h *q5MaxBid) process(ctx context.Context, argsTmp interface{},
) (map[string]uint64, *common.FnOutput) {
	args := argsTmp.(*q5MaxBidProcessArgs)
	gotMsgs, err := args.src.Consume(ctx, args.parNum)
	if err != nil {
		if xerrors.Is(err, errors.ErrStreamSourceTimeout) {
			return h.currentOffset, &common.FnOutput{
				Success: true,
				Message: err.Error(),
			}
		}
		return h.currentOffset, &common.FnOutput{
			Success: false,
			Message: fmt.Sprintf("consume err: %v", err),
		}
	}

	for _, msg := range gotMsgs {
		if msg.Msg.Value == nil {
			continue
		}
		aic := msg.Msg.Value.(*ntypes.AuctionIdCount)
		ts, err := aic.ExtractStreamTime()
		if err != nil {
			return h.currentOffset, &common.FnOutput{
				Success: false,
				Message: fmt.Sprintf("fail to extract timestamp: %v", err),
			}
		}
		msg.Msg.Timestamp = ts
		// fmt.Fprintf(os.Stderr, "got msg with key: %v, val: %v, ts: %v\n", msg.Msg.Key, msg.Msg.Value, msg.Msg.Timestamp)
		h.currentOffset[args.src.TopicName()] = msg.LogSeqNum
		_, err = args.maxBid.ProcessAndReturn(ctx, msg.Msg)
		if err != nil {
			return h.currentOffset, &common.FnOutput{
				Success: false,
				Message: fmt.Sprintf("maxBid err: %v", err),
			}
		}
		joinedOutput, err := args.stJoin.ProcessAndReturn(ctx, msg.Msg)
		if err != nil {
			return h.currentOffset, &common.FnOutput{
				Success: false,
				Message: fmt.Sprintf("joined err: %v", err),
			}
		}
		filteredMx, err := args.chooseMaxCnt.ProcessAndReturn(ctx, joinedOutput[0])
		if err != nil {
			return h.currentOffset, &common.FnOutput{
				Success: false,
				Message: fmt.Sprintf("filteredMx err: %v", err),
			}
		}
		for _, filtered := range filteredMx {
			err = args.sink.Sink(ctx, filtered, args.parNum, false)
			if err != nil {
				return h.currentOffset, &common.FnOutput{
					Success: false,
					Message: fmt.Sprintf("sink err: %v", err),
				}
			}
		}
	}
	return h.currentOffset, nil
}

func (h *q5MaxBid) processQ5MaxBid(ctx context.Context, sp *common.QueryInput) *common.FnOutput {
	input_stream, output_stream, err := benchutil.GetShardedInputOutputStreams(ctx, h.env, sp, true)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: err.Error(),
		}
	}

	var seSerde commtypes.Serde
	var vtSerde commtypes.Serde
	var aucIdCountSerde commtypes.Serde
	var aucIdCountMaxSerde commtypes.Serde
	if sp.SerdeFormat == uint8(commtypes.JSON) {
		seSerde = ntypes.StartEndTimeJSONSerde{}
		aucIdCountSerde = ntypes.AuctionIdCountJSONSerde{}
		vtSerde = commtypes.ValueTimestampJSONSerde{
			ValJSONSerde: commtypes.Uint64Serde{},
		}
		aucIdCountMaxSerde = ntypes.AuctionIdCntMaxJSONSerde{}
	} else if sp.SerdeFormat == uint8(commtypes.MSGP) {
		seSerde = ntypes.StartEndTimeMsgpSerde{}
		aucIdCountSerde = ntypes.AuctionIdCountMsgpSerde{}
		vtSerde = commtypes.ValueTimestampMsgpSerde{
			ValMsgpSerde: commtypes.Uint64Serde{},
		}
		aucIdCountMaxSerde = ntypes.AuctionIdCntMaxMsgpSerde{}
	} else {
		return &common.FnOutput{
			Success: false,
			Message: fmt.Sprintf("serde format should be either json or msgp; but %v is given", sp.SerdeFormat),
		}
	}
	msgSerde, err := commtypes.GetMsgSerde(sp.SerdeFormat)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	src, sink, err := h.getSrcSink(ctx, sp,
		input_stream, output_stream, seSerde, aucIdCountSerde, aucIdCountMaxSerde, msgSerde)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	maxBidStoreName := "maxBidsKVStore"
	var kvstore store.KeyValueStore
	if sp.TableType == uint8(store.IN_MEM) {
		mp := &store.MaterializeParam{
			KeySerde:   seSerde,
			ValueSerde: vtSerde,
			MsgSerde:   msgSerde,
			StoreName:  maxBidStoreName,
			Changelog:  output_stream,
			ParNum:     sp.ParNum,
		}
		inMemStore := store.NewInMemoryKeyValueStore(mp.StoreName, func(a, b treemap.Key) int {
			ka := a.(*ntypes.StartEndTime)
			kb := b.(*ntypes.StartEndTime)
			return ntypes.CompareStartEndTime(ka, kb)
		})
		kvstore = store.NewKeyValueStoreWithChangelog(mp, inMemStore, false)
	} else if sp.TableType == uint8(store.MONGODB) {
		kvstore, err = store.NewMongoDBKeyValueStore(ctx, &store.MongoDBConfig{
			Addr:           sp.MongoAddr,
			CollectionName: maxBidStoreName,
			DBName:         maxBidStoreName,
			KeySerde:       seSerde,
			ValueSerde:     vtSerde,
		})
		if err != nil {
			return &common.FnOutput{Success: false, Message: err.Error()}
		}
	} else {
		panic("unrecognized table type")
	}
	maxBid := processor.NewMeteredProcessor(processor.NewStreamAggregateProcessor(kvstore, processor.InitializerFunc(func() interface{} {
		return uint64(0)
	}), processor.AggregatorFunc(func(key, value, aggregate interface{}) interface{} {
		v := value.(*ntypes.AuctionIdCount)
		agg := aggregate.(uint64)
		if v.Count > agg {
			return v.Count
		}
		return agg
	})))
	stJoin := processor.NewMeteredProcessor(processor.NewStreamTableJoinProcessor(maxBidStoreName, kvstore,
		processor.ValueJoinerWithKeyFunc(
			func(readOnlyKey interface{}, leftValue interface{}, rightValue interface{}) interface{} {
				lv := leftValue.(*ntypes.AuctionIdCount)
				rv := rightValue.(commtypes.ValueTimestamp)
				return &ntypes.AuctionIdCntMax{
					AucId:  lv.AucId,
					Count:  lv.Count,
					MaxCnt: rv.Value.(uint64),
				}
			})))
	chooseMaxCnt := processor.NewMeteredProcessor(
		processor.NewStreamFilterProcessor(
			processor.PredicateFunc(func(msg *commtypes.Message) (bool, error) {
				v := msg.Value.(*ntypes.AuctionIdCntMax)
				return v.Count >= v.MaxCnt, nil
			})))

	procArgs := &q5MaxBidProcessArgs{
		maxBid:        maxBid,
		stJoin:        stJoin,
		chooseMaxCnt:  chooseMaxCnt,
		src:           src,
		sink:          sink,
		output_stream: output_stream,
		parNum:        sp.ParNum,
		trackParFunc:  sharedlog_stream.DefaultTrackSubstreamFunc,
	}

	task := sharedlog_stream.StreamTask{
		ProcessFunc: h.process,
	}

	if sp.EnableTransaction {
		srcs := make(map[string]processor.Source)
		srcs[sp.InputTopicNames[0]] = src
		var kvc []*store.KVStoreChangelog
		if sp.TableType == uint8(store.IN_MEM) {
			kvstore_mem := kvstore.(*store.KeyValueStoreWithChangelog)
			mp := kvstore_mem.MaterializeParam()
			kvc = []*store.KVStoreChangelog{
				store.NewKVStoreChangelog(kvstore, mp.Changelog,
					mp.KeySerde, mp.ValueSerde, sp.ParNum),
			}
		} else if sp.TableType == uint8(store.MONGODB) {
			// TODO: MONGODB
			kvc = nil
		} else {
			panic("unrecognized table type")
		}
		streamTaskArgs := sharedlog_stream.StreamTaskArgsTransaction{
			ProcArgs:     procArgs,
			Env:          h.env,
			Srcs:         srcs,
			OutputStream: output_stream,
			QueryInput:   sp,
			TransactionalId: fmt.Sprintf("q5MaxBid-%s-%d-%s",
				sp.InputTopicNames[0], sp.ParNum, sp.OutputTopicName),
			FixedOutParNum:        sp.ParNum,
			KVChangelogs:          kvc,
			WindowStoreChangelogs: nil,
			MsgSerde:              msgSerde,
			CHash:                 nil,
			CHashMu:               nil,
		}
		ret := sharedlog_stream.SetupManagersAndProcessTransactional(ctx, h.env, &streamTaskArgs,
			func(procArgs interface{}, trackParFunc sharedlog_stream.TrackKeySubStreamFunc) {
				procArgs.(*q5MaxBidProcessArgs).trackParFunc = trackParFunc
			}, &task)
		if ret != nil && ret.Success {
			ret.Latencies["src"] = src.GetLatency()
			ret.Latencies["sink"] = sink.GetLatency()
			ret.Latencies["maxBid"] = maxBid.GetLatency()
			ret.Latencies["stJoin"] = stJoin.GetLatency()
			ret.Latencies["chooseMaxCnt"] = chooseMaxCnt.GetLatency()
			ret.Consumed["src"] = src.GetCount()
		}
		return ret
	}
	streamTaskArgs := sharedlog_stream.StreamTaskArgs{
		ProcArgs: procArgs,
		Duration: time.Duration(sp.Duration) * time.Second,
	}
	ret := task.Process(ctx, &streamTaskArgs)
	if ret != nil && ret.Success {
		ret.Latencies["src"] = src.GetLatency()
		ret.Latencies["sink"] = sink.GetLatency()
		ret.Latencies["maxBid"] = maxBid.GetLatency()
		ret.Latencies["stJoin"] = stJoin.GetLatency()
		ret.Latencies["chooseMaxCnt"] = chooseMaxCnt.GetLatency()
		ret.Consumed["src"] = src.GetCount()
	}
	return ret
}
