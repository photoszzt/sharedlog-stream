package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"sharedlog-stream/benchmark/common"
	ntypes "sharedlog-stream/benchmark/nexmark/pkg/nexmark/types"
	"sharedlog-stream/benchmark/nexmark/pkg/nexmark/utils"
	"sharedlog-stream/pkg/debug"
	"sharedlog-stream/pkg/hash"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/stream/processor"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"sharedlog-stream/pkg/stream/processor/store"
	"sharedlog-stream/pkg/transaction"
	"sharedlog-stream/pkg/treemap"
	"sync"
	"time"

	"cs.utexas.edu/zjia/faas/types"
)

type q3JoinTableHandler struct {
	env     types.Environment
	cHashMu sync.RWMutex
	cHash   *hash.ConsistentHash

	offMu    sync.Mutex
	funcName string
}

func NewQ3JoinTableHandler(env types.Environment, funcName string) types.FuncHandler {
	return &q3JoinTableHandler{
		env:      env,
		cHash:    hash.NewConsistentHash(),
		funcName: funcName,
	}
}

func (h *q3JoinTableHandler) Call(ctx context.Context, input []byte) ([]byte, error) {
	parsedInput := &common.QueryInput{}
	err := json.Unmarshal(input, parsedInput)
	if err != nil {
		return nil, err
	}
	output := h.Query3JoinTable(ctx, parsedInput)
	encodedOutput, err := json.Marshal(output)
	if err != nil {
		panic(err)
	}
	// fmt.Printf("query 3 output: %v\n", encodedOutput)
	return utils.CompressData(encodedOutput), nil
}

func (h *q3JoinTableHandler) process(ctx context.Context,
	t *transaction.StreamTask,
	argsTmp interface{},
) (map[string]uint64, *common.FnOutput) {
	args := argsTmp.(*q3JoinTableProcessArgs)
	var wg sync.WaitGroup

	personsOutChan := make(chan *common.FnOutput)
	auctionsOutChan := make(chan *common.FnOutput)
	completed := uint32(0)
	wg.Add(1)
	joinProcPerson := &joinProcArgs{
		src:           args.personSrc,
		sink:          args.sink,
		wg:            &wg,
		parNum:        args.parNum,
		runner:        args.pJoinA,
		offMu:         &h.offMu,
		currentOffset: t.CurrentOffset,
		trackParFunc:  args.trackParFunc,
		cHashMu:       &h.cHashMu,
		cHash:         h.cHash,
	}
	pctx := context.WithValue(ctx, "id", "person")
	go joinProc(pctx, personsOutChan, joinProcPerson)
	wg.Add(1)
	joinProgArgsAuction := &joinProcArgs{
		src:           args.auctionSrc,
		sink:          args.sink,
		wg:            &wg,
		parNum:        args.parNum,
		runner:        args.aJoinP,
		offMu:         &h.offMu,
		currentOffset: t.CurrentOffset,
		trackParFunc:  args.trackParFunc,
		cHashMu:       &h.cHashMu,
		cHash:         h.cHash,
	}
	actx := context.WithValue(ctx, "id", "auction")
	go joinProc(actx, auctionsOutChan, joinProgArgsAuction)
	var pOut *common.FnOutput = nil
	var aOut *common.FnOutput = nil
L:
	for {
		select {
		case personOutput := <-personsOutChan:
			pOut = personOutput
			completed += 1
			if completed == 2 {
				break L
			}
		case auctionOutput := <-auctionsOutChan:
			aOut = auctionOutput
			completed += 1
			if completed == 2 {
				break L
			}
		}
	}
	wg.Wait()
	if pOut != nil {
		// debug.Fprintf(os.Stderr, "pOut %v\n", pOut)
		if aOut != nil {
			// debug.Fprintf(os.Stderr, "aOut %v\n", aOut)
			succ := pOut.Success && aOut.Success
			return t.CurrentOffset, &common.FnOutput{Success: succ, Message: pOut.Message + "," + aOut.Message}
		}
		if pOut.Success {
			return t.CurrentOffset, nil
		}
		return t.CurrentOffset, pOut
	} else if aOut != nil {
		// debug.Fprintf(os.Stderr, "aOut %v\n", aOut)
		if aOut.Success {
			return t.CurrentOffset, nil
		}
		return t.CurrentOffset, aOut
	}
	return t.CurrentOffset, nil
}

func getInOutStreams(
	ctx context.Context,
	env types.Environment,
	input *common.QueryInput,
	input_in_tran bool,
) (*sharedlog_stream.ShardedSharedLogStream, /* auction */
	*sharedlog_stream.ShardedSharedLogStream, /* person */
	*sharedlog_stream.ShardedSharedLogStream, /* output */
	error,
) {
	inputStream1, err := sharedlog_stream.NewShardedSharedLogStream(env, input.InputTopicNames[0], input.NumInPartition,
		commtypes.SerdeFormat(input.SerdeFormat))
	if err != nil {
		return nil, nil, nil, fmt.Errorf("NewSharedlogStream for input stream failed: %v", err)
	}
	inputStream2, err := sharedlog_stream.NewShardedSharedLogStream(env, input.InputTopicNames[1], input.NumInPartition,
		commtypes.SerdeFormat(input.SerdeFormat))
	if err != nil {
		return nil, nil, nil, fmt.Errorf("NewSharedlogStream for input stream failed: %v", err)
	}
	outputStream, err := sharedlog_stream.NewShardedSharedLogStream(env, input.OutputTopicNames[0], input.NumOutPartitions[0],
		commtypes.SerdeFormat(input.SerdeFormat))
	if err != nil {
		return nil, nil, nil, fmt.Errorf("NewSharedlogStream for output stream failed: %v", err)
	}
	if input.EnableTransaction {
		inputStream1.SetInTransaction(input_in_tran)
		inputStream2.SetInTransaction(input_in_tran)
		outputStream.SetInTransaction(true)
	} else {
		inputStream1.SetInTransaction(false)
		inputStream2.SetInTransaction(false)
		outputStream.SetInTransaction(false)
	}
	return inputStream1, inputStream2, outputStream, nil
}

type srcSinkSerde struct {
	src1      *processor.MeteredSource
	src2      *processor.MeteredSource
	sink      *processor.ConcurrentMeteredSink
	msgSerde  commtypes.MsgSerde
	keySerdes []commtypes.Serde
	valSerdes []commtypes.Serde
}

func (h *q3JoinTableHandler) getSrcSink(ctx context.Context, sp *common.QueryInput,
	stream1 *sharedlog_stream.ShardedSharedLogStream,
	stream2 *sharedlog_stream.ShardedSharedLogStream,
	outputStream *sharedlog_stream.ShardedSharedLogStream,
) (*srcSinkSerde,
	error,
) {
	msgSerde, err := commtypes.GetMsgSerde(sp.SerdeFormat)
	if err != nil {
		return nil, fmt.Errorf("get msg serde err: %v", err)
	}

	eventSerde, err := getEventSerde(sp.SerdeFormat)
	if err != nil {
		return nil, fmt.Errorf("get event serde err: %v", err)
	}
	auctionsConfig := &sharedlog_stream.StreamSourceConfig{
		Timeout:      common.SrcConsumeTimeout,
		KeyDecoder:   commtypes.Uint64Serde{},
		ValueDecoder: eventSerde,
		MsgDecoder:   msgSerde,
	}
	personsConfig := &sharedlog_stream.StreamSourceConfig{
		Timeout:      common.SrcConsumeTimeout,
		KeyDecoder:   commtypes.Uint64Serde{},
		ValueDecoder: eventSerde,
		MsgDecoder:   msgSerde,
	}
	var ncsiSerde commtypes.Serde
	if sp.SerdeFormat == uint8(commtypes.JSON) {
		ncsiSerde = ntypes.NameCityStateIdJSONSerde{}
	} else {
		ncsiSerde = ntypes.NameCityStateIdMsgpSerde{}
	}
	outConfig := &sharedlog_stream.StreamSinkConfig{
		KeySerde:   commtypes.Uint64Serde{},
		ValueSerde: ncsiSerde,
		MsgSerde:   msgSerde,
	}

	src1 := processor.NewMeteredSource(sharedlog_stream.NewShardedSharedLogStreamSource(stream1, auctionsConfig))
	src2 := processor.NewMeteredSource(sharedlog_stream.NewShardedSharedLogStreamSource(stream2, personsConfig))
	sink := processor.NewConcurrentMeteredSink(sharedlog_stream.NewShardedSharedLogStreamSink(outputStream, outConfig))
	sink.MarkFinalOutput()
	sss := &srcSinkSerde{
		src1:      src1,
		src2:      src2,
		sink:      sink,
		keySerdes: []commtypes.Serde{commtypes.Uint64Serde{}, commtypes.Uint64Serde{}},
		valSerdes: []commtypes.Serde{eventSerde, eventSerde},
		msgSerde:  msgSerde,
	}
	return sss, nil
}

type kvtables struct {
	toTab1 *processor.MeteredProcessor
	tab1   store.KeyValueStore
	toTab2 *processor.MeteredProcessor
	tab2   store.KeyValueStore
}

func (h *q3JoinTableHandler) setupTables(ctx context.Context,
	tabType store.TABLE_TYPE,
	sss *srcSinkSerde,
	serdeFormat commtypes.SerdeFormat,
	mongoAddr string,
) (
	*kvtables,
	error,
) {
	if tabType == store.IN_MEM {
		compare := func(a, b treemap.Key) int {
			valA := a.(uint64)
			valB := b.(uint64)
			if valA < valB {
				return -1
			} else if valA == valB {
				return 0
			} else {
				return 1
			}
		}

		toAuctionsTable, auctionsStore, err := processor.ToInMemKVTable("auctionsBySellerIDStore", compare)
		if err != nil {
			return nil, fmt.Errorf("toAucTab err: %v", err)
		}

		toPersonsTable, personsStore, err := processor.ToInMemKVTable("personsByIDStore", compare)
		if err != nil {
			return nil, fmt.Errorf("toPersonsTab err: %v", err)
		}
		return &kvtables{toAuctionsTable, auctionsStore, toPersonsTable, personsStore}, nil
	} else if tabType == store.MONGODB {
		var vtSerde0 commtypes.Serde
		var vtSerde1 commtypes.Serde
		if serdeFormat == commtypes.JSON {
			vtSerde0 = commtypes.ValueTimestampJSONSerde{
				ValJSONSerde: sss.valSerdes[0],
			}
			vtSerde1 = commtypes.ValueTimestampJSONSerde{
				ValJSONSerde: sss.valSerdes[1],
			}
		} else if serdeFormat == commtypes.MSGP {
			vtSerde0 = commtypes.ValueTimestampMsgpSerde{
				ValMsgpSerde: sss.valSerdes[0],
			}
			vtSerde1 = commtypes.ValueTimestampMsgpSerde{
				ValMsgpSerde: sss.valSerdes[1],
			}
		} else {
			panic("unrecognized serde format")
		}
		client, err := store.InitMongoDBClient(ctx, mongoAddr)
		if err != nil {
			return nil, err
		}
		toAuctionsTable, auctionsStore, err := processor.ToMongoDBKVTable(ctx, "auctionsBySellerIDStore",
			client, sss.keySerdes[0], vtSerde0)
		if err != nil {
			return nil, err
		}
		toPersonsTable, personsStore, err := processor.ToMongoDBKVTable(ctx, "personsByIDStore",
			client, sss.keySerdes[1], vtSerde1)
		if err != nil {
			return nil, err
		}
		return &kvtables{toAuctionsTable, auctionsStore, toPersonsTable, personsStore}, nil
	} else {
		return nil, fmt.Errorf("unrecognized table type")
	}
}

type q3JoinTableProcessArgs struct {
	personSrc        *processor.MeteredSource
	auctionSrc       *processor.MeteredSource
	sink             *processor.ConcurrentMeteredSink
	pJoinA           JoinWorkerFunc
	aJoinP           JoinWorkerFunc
	trackParFunc     transaction.TrackKeySubStreamFunc
	recordFinishFunc transaction.RecordPrevInstanceFinishFunc
	funcName         string
	curEpoch         uint64
	parNum           uint8
}

func (a *q3JoinTableProcessArgs) ParNum() uint8    { return a.parNum }
func (a *q3JoinTableProcessArgs) CurEpoch() uint64 { return a.curEpoch }
func (a *q3JoinTableProcessArgs) FuncName() string { return a.funcName }
func (a *q3JoinTableProcessArgs) RecordFinishFunc() func(ctx context.Context, funcName string, instanceId uint8) error {
	return a.recordFinishFunc
}

func (h *q3JoinTableHandler) Query3JoinTable(ctx context.Context, sp *common.QueryInput) *common.FnOutput {
	auctionsStream, personsStream, outputStream, err := getInOutStreams(ctx, h.env, sp, true)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: fmt.Sprintf("get input output err: %v", err),
		}
	}
	sss, err := h.getSrcSink(ctx, sp, auctionsStream,
		personsStream, outputStream)
	if err != nil {
		return &common.FnOutput{Success: false, Message: fmt.Sprintf("getSrcSink err: %v\n", err)}
	}

	kvtabs, err := h.setupTables(ctx, store.TABLE_TYPE(sp.TableType), sss, commtypes.SerdeFormat(sp.SerdeFormat), sp.MongoAddr)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	joiner := processor.ValueJoinerWithKeyFunc(
		func(readOnlyKey interface{}, leftVal interface{}, rightVal interface{}) interface{} {
			var event *ntypes.Event
			vt, ok := rightVal.(commtypes.ValueTimestamp)
			if ok {
				event = vt.Value.(*ntypes.Event)
			} else {
				event = rightVal.(*ntypes.Event)
			}
			return &ntypes.NameCityStateId{
				Name:  event.NewPerson.Name,
				City:  event.NewPerson.City,
				State: event.NewPerson.State,
				ID:    event.NewPerson.ID,
			}
		})

	auctionJoinsPersons := processor.NewMeteredProcessor(
		processor.NewTableTableJoinProcessor(kvtabs.tab2.Name(), kvtabs.tab2, joiner))

	personJoinsAuctions := processor.NewMeteredProcessor(
		processor.NewTableTableJoinProcessor(kvtabs.tab1.Name(), kvtabs.tab1,
			processor.ReverseValueJoinerWithKey(joiner)))

	aJoinP := JoinWorkerFunc(func(c context.Context, m commtypes.Message) ([]commtypes.Message, error) {
		// msg is auction
		_, err := kvtabs.toTab1.ProcessAndReturn(ctx, m)
		if err != nil {
			return nil, fmt.Errorf("ToTabA err: %v", err)
		}
		msgs, err := auctionJoinsPersons.ProcessAndReturn(ctx, m)
		if err != nil {
			err = fmt.Errorf("aJoinP err: %v", err)
		}
		return msgs, err
	})
	pJoinA := JoinWorkerFunc(func(ctx context.Context, m commtypes.Message) ([]commtypes.Message, error) {
		// msg is person
		_, err := kvtabs.toTab2.ProcessAndReturn(ctx, m)
		if err != nil {
			return nil, fmt.Errorf("ToTabP err: %v", err)
		}
		msgs, err := personJoinsAuctions.ProcessAndReturn(ctx, m)
		if err != nil {
			err = fmt.Errorf("pJoinA err: %v", err)
		}
		return msgs, err
	})

	debug.Assert(len(sp.NumOutPartitions) == 1 && len(sp.OutputTopicNames) == 1,
		"expected only one output stream")
	transaction.SetupConsistentHash(&h.cHashMu, h.cHash, sp.NumOutPartitions[0])

	debug.Assert(sp.ScaleEpoch != 0, "scale epoch should start from 1")
	procArgs := &q3JoinTableProcessArgs{
		auctionSrc:       sss.src1,
		personSrc:        sss.src2,
		sink:             sss.sink,
		aJoinP:           aJoinP,
		pJoinA:           pJoinA,
		parNum:           sp.ParNum,
		trackParFunc:     transaction.DefaultTrackSubstreamFunc,
		recordFinishFunc: transaction.DefaultRecordPrevInstanceFinishFunc,
		curEpoch:         sp.ScaleEpoch,
		funcName:         h.funcName,
	}

	task := transaction.StreamTask{
		ProcessFunc:   h.process,
		CurrentOffset: make(map[string]uint64),
	}
	if sp.EnableTransaction {
		var kvchangelogs []*transaction.KVStoreChangelog
		if sp.TableType == uint8(store.IN_MEM) {
			kvchangelogs = []*transaction.KVStoreChangelog{
				transaction.NewKVStoreChangelog(kvtabs.tab1, auctionsStream, sss.keySerdes[0], sss.valSerdes[0], sp.ParNum),
				transaction.NewKVStoreChangelog(kvtabs.tab2, personsStream, sss.keySerdes[1], sss.valSerdes[1], sp.ParNum),
			}
		} else if sp.TableType == uint8(store.MONGODB) {
			kvchangelogs = []*transaction.KVStoreChangelog{
				transaction.NewKVStoreChangelogForExternalStore(kvtabs.tab1, auctionsStream, joinProcSerialWithoutSink,
					&joinProcWithoutSinkArgs{
						src:    sss.src1.InnerSource(),
						parNum: sp.ParNum,
						runner: aJoinP,
					}, fmt.Sprintf("%s-%s-%d", h.funcName, kvtabs.tab1.Name(), sp.ParNum), sp.ParNum),
				transaction.NewKVStoreChangelogForExternalStore(kvtabs.tab2, personsStream, joinProcSerialWithoutSink,
					&joinProcWithoutSinkArgs{
						src:    sss.src2.InnerSource(),
						parNum: sp.ParNum,
						runner: pJoinA,
					}, fmt.Sprintf("%s-%s-%d", h.funcName, kvtabs.tab2.Name(), sp.ParNum), sp.ParNum),
			}
		}
		streamTaskArgs := transaction.StreamTaskArgsTransaction{
			ProcArgs:              procArgs,
			Env:                   h.env,
			MsgSerde:              sss.msgSerde,
			Srcs:                  map[string]processor.Source{auctionsStream.TopicName(): sss.src1, personsStream.TopicName(): sss.src2},
			OutputStreams:         []*sharedlog_stream.ShardedSharedLogStream{outputStream},
			QueryInput:            sp,
			TransactionalId:       fmt.Sprintf("%s-%d-%s", h.funcName, sp.ParNum, sp.OutputTopicNames[0]),
			KVChangelogs:          kvchangelogs,
			WindowStoreChangelogs: nil,
			FixedOutParNum:        0,
		}
		ret := transaction.SetupManagersAndProcessTransactional(ctx, h.env, &streamTaskArgs,
			func(procArgs interface{}, trackParFunc transaction.TrackKeySubStreamFunc, recordFinishFunc transaction.RecordPrevInstanceFinishFunc) {
				procArgs.(*q3JoinTableProcessArgs).trackParFunc = trackParFunc
				procArgs.(*q3JoinTableProcessArgs).recordFinishFunc = recordFinishFunc
			}, &task)
		if ret != nil && ret.Success {
			ret.Latencies["auctionsSrc"] = procArgs.auctionSrc.GetLatency()
			ret.Latencies["personsSrc"] = procArgs.personSrc.GetLatency()
			ret.Latencies["toAuctionsTable"] = kvtabs.toTab1.GetLatency()
			ret.Latencies["toPersonsTable"] = kvtabs.toTab2.GetLatency()
			ret.Latencies["personJoinsAuctions"] = personJoinsAuctions.GetLatency()
			ret.Latencies["auctionJoinsPersons"] = auctionJoinsPersons.GetLatency()
			ret.Latencies["sink"] = sss.sink.GetLatency()
			ret.Latencies["eventTimeLatency"] = sss.sink.GetEventTimeLatency()
			ret.Consumed["auctionsSrc"] = procArgs.auctionSrc.GetCount()
			ret.Consumed["personsSrc"] = procArgs.personSrc.GetCount()
		}
		return ret
	}
	streamTaskArgs := transaction.StreamTaskArgs{
		ProcArgs: procArgs,
		Duration: time.Duration(sp.Duration) * time.Second,
	}
	ret := task.Process(ctx, &streamTaskArgs)
	if ret != nil && ret.Success {
		ret.Latencies["auctionsSrc"] = procArgs.auctionSrc.GetLatency()
		ret.Latencies["personsSrc"] = procArgs.personSrc.GetLatency()
		ret.Latencies["toAuctionsTable"] = kvtabs.toTab1.GetLatency()
		ret.Latencies["toPersonsTable"] = kvtabs.toTab2.GetLatency()
		ret.Latencies["personJoinsAuctions"] = personJoinsAuctions.GetLatency()
		ret.Latencies["auctionJoinsPersons"] = auctionJoinsPersons.GetLatency()
		ret.Latencies["sink"] = sss.sink.GetLatency()
		ret.Latencies["eventTimeLatency"] = sss.sink.GetEventTimeLatency()
		ret.Consumed["auctionsSrc"] = procArgs.auctionSrc.GetCount()
		ret.Consumed["personsSrc"] = procArgs.personSrc.GetCount()
	}
	return ret
}
