package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/benchmark/nexmark/pkg/nexmark/utils"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/transaction"
	"sharedlog-stream/pkg/txn_data"

	ntypes "sharedlog-stream/benchmark/nexmark/pkg/nexmark/types"
	"sharedlog-stream/pkg/stream/processor/commtypes"

	"sharedlog-stream/benchmark/nexmark/pkg/nexmark/generator"

	"cs.utexas.edu/zjia/faas/types"
)

type nexmarkSourceHandler struct {
	env      types.Environment
	funcName string
	bufPush  bool
	// tStart   time.Time
}

func NewNexmarkSource(env types.Environment, funcName string) types.FuncHandler {
	bufPush_str := os.Getenv("BUFPUSH")
	bufPush := false
	if bufPush_str == "true" || bufPush_str == "1" {
		bufPush = true
	}
	if bufPush {
		return &nexmarkSourceHandler{
			env:      env,
			funcName: funcName,
			bufPush:  bufPush,
		}
	}
	return &nexmarkSourceHandler{
		env:      env,
		funcName: funcName,
		bufPush:  bufPush,
	}
}

func (h *nexmarkSourceHandler) Call(ctx context.Context, input []byte) ([]byte, error) {
	inputConfig := &ntypes.NexMarkConfigInput{}
	err := json.Unmarshal(input, inputConfig)
	if err != nil {
		return nil, err
	}
	output := h.eventGeneration(ctx, inputConfig)
	encodedOutput, err := json.Marshal(output)
	if err != nil {
		panic(err)
	}
	return utils.CompressData(encodedOutput), nil
}

/*
type sinkMsg struct {
	encoded []byte
	parNum  uint8
}

func closeAllChanAndWait(inChans []chan sinkMsg, g *errgroup.Group, err error) *common.FnOutput {
	for _, inChan := range inChans {
		close(inChan)
	}
	gerr := g.Wait()
	if gerr != nil {
		return &common.FnOutput{
			Success: false,
			Message: fmt.Sprintf("next event failed : %v and wait failed: %v\n", err, gerr),
		}
	}
	return nil
}
*/

func encodeEvent(event *ntypes.Event,
	eventSerde commtypes.Serde,
	msgSerde commtypes.MsgSerde,
) ([]byte, error) {
	encoded, err := eventSerde.Encode(event)
	if err != nil {
		return nil, err
	}
	msgEncoded, err := msgSerde.Encode(nil, encoded)
	if err != nil {
		return nil, err
	}
	return msgEncoded, nil
}

const (
	FLUSH_DURATION = time.Duration(100) * time.Millisecond
)

func (h *nexmarkSourceHandler) getSerde(serdeFormat uint8) (commtypes.Serde, commtypes.Serde, commtypes.MsgSerde, error) {
	var eventSerde commtypes.Serde
	var msgSerde commtypes.MsgSerde
	var txnMarkerSerde commtypes.Serde
	if serdeFormat == uint8(commtypes.JSON) {
		eventSerde = ntypes.EventJSONSerde{}
		msgSerde = commtypes.MessageSerializedJSONSerde{}
		txnMarkerSerde = txn_data.TxnMarkerJSONSerde{}
	} else if serdeFormat == uint8(commtypes.MSGP) {
		eventSerde = ntypes.EventMsgpSerde{}
		msgSerde = commtypes.MessageSerializedMsgpSerde{}
		txnMarkerSerde = txn_data.TxnMarkerMsgpSerde{}
	} else {
		return nil, nil, nil, fmt.Errorf("serde format should be either json or msgp; but %v is given", serdeFormat)
	}
	return eventSerde, txnMarkerSerde, msgSerde, nil
}

type nexmarkSrcProcArgs struct {
	eventSerde        commtypes.Serde
	msgSerde          commtypes.MsgSerde
	channel_url_cache map[uint32]*generator.ChannelUrl
	eventGenerator    *generator.NexmarkGenerator
	msgChan           chan sharedlog_stream.PayloadToPush
	latencies         []int
	idx               int
	numPartition      uint8
}

func (h *nexmarkSourceHandler) process(ctx context.Context, args *nexmarkSrcProcArgs) *common.FnOutput {
	nowT := time.Now()
	nowMs := nowT.UnixMilli()
	nextEvent, err := args.eventGenerator.NextEvent(ctx, args.channel_url_cache)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: fmt.Sprintf("next event failed: %v\n", err),
		}
	}
	wtsMs := nextEvent.WallclockTimestamp
	if wtsMs > nowMs {
		// fmt.Fprintf(os.Stderr, "sleep %v ms to generate event\n", wtsSec-now)
		time.Sleep(time.Duration(wtsMs-nowMs) * time.Millisecond)
	}
	// fmt.Fprintf(os.Stderr, "gen event with ts: %v\n", nextEvent.EventTimestamp)
	msgEncoded, err := encodeEvent(nextEvent.Event, args.eventSerde, args.msgSerde)
	if err != nil {
		return &common.FnOutput{Success: false, Message: fmt.Sprintf("msg serialization failed: %v", err)}
	}
	// fmt.Fprintf(os.Stderr, "msg: %v\n", string(msgEncoded))
	args.idx += 1
	parNum := args.idx
	parNum = parNum % int(args.numPartition)

	args.msgChan <- sharedlog_stream.PayloadToPush{Payload: msgEncoded, Partitions: []uint8{uint8(parNum)}, IsControl: false}
	/*
		if h.bufPush {
			err = args.stream.BufPushNoLock(ctx, msgEncoded, uint8(parNum))
			if time.Since(h.tStart) >= FLUSH_DURATION {
				args.stream.FlushNoLock(ctx)
				h.tStart = time.Now()
			}
		} else {
			_, err = args.stream.Push(ctx, msgEncoded, uint8(parNum), false, false)
		}
		if err != nil {
			return &common.FnOutput{Success: false, Message: fmt.Sprintf("stream push failed: %v", err)}
		}
	*/
	// fmt.Fprintf(os.Stderr, "inserted to pos: 0x%x\n", pos)

	/*
		inChan := args.inChans[args.idx]
		sinkMsg := sinkMsg{
			encoded: msgEncoded,
			parNum:  uint8(args.idx),
		}
		inChan <- sinkMsg
	*/
	elapsed := time.Since(nowT)
	args.latencies = append(args.latencies, int(elapsed.Microseconds()))
	return nil
}

func (h *nexmarkSourceHandler) eventGeneration(ctx context.Context, inputConfig *ntypes.NexMarkConfigInput) *common.FnOutput {
	stream, err := sharedlog_stream.NewShardedSharedLogStream(h.env, inputConfig.TopicName, inputConfig.NumOutPartition,
		commtypes.SerdeFormat(inputConfig.SerdeFormat))
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: fmt.Sprintf("fail to create output stream: %v", err),
		}
	}
	nexmarkConfig, err := ntypes.ConvertToNexmarkConfiguration(inputConfig)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: fmt.Sprintf("fail to convert to nexmark configuration: %v", err),
		}
	}
	generatorConfig := generator.NewGeneratorConfig(nexmarkConfig, time.Now().UnixMilli(), 1, uint64(nexmarkConfig.NumEvents), 1)
	fmt.Fprint(os.Stderr, "Generator config: \n")
	fmt.Fprintf(os.Stderr, "\tInterEventDelayUs: %v\n", generatorConfig.InterEventDelayUs)
	fmt.Fprintf(os.Stderr, "\tEventPerEpoch    : %v\n", generatorConfig.EventPerEpoch)
	fmt.Fprintf(os.Stderr, "\tMaxEvents        : %v\n", generatorConfig.MaxEvents)
	fmt.Fprintf(os.Stderr, "\tFirstEventNumber : %v\n", generatorConfig.FirstEventNumber)
	fmt.Fprintf(os.Stderr, "\tEpochPeriodMs    : %v\n", generatorConfig.EpochPeriodMs)
	fmt.Fprintf(os.Stderr, "\tStepLengthSec    : %v\n", generatorConfig.StepLengthSec)
	fmt.Fprintf(os.Stderr, "\tBaseTime         : %v\n", generatorConfig.BaseTime)
	fmt.Fprintf(os.Stderr, "\tFirstEventId     : %v\n", generatorConfig.FirstEventId)
	fmt.Fprintf(os.Stderr, "\tTotalProportion  : %v\n", generatorConfig.TotalProportion)
	fmt.Fprintf(os.Stderr, "\tBidProportion    : %v\n", generatorConfig.BidProportion)
	fmt.Fprintf(os.Stderr, "\tAuctionProportion: %d\n", generatorConfig.AuctionProportion)
	fmt.Fprintf(os.Stderr, "\tPersonProportion : %d\n", generatorConfig.PersonProportion)

	fmt.Fprint(os.Stderr, "Nexmark config: \n")
	fmt.Fprintf(os.Stderr, "\tNumEvents            : %v\n", generatorConfig.Configuration.NumEvents)
	fmt.Fprintf(os.Stderr, "\tNumEventGenerators   : %v\n", generatorConfig.Configuration.NumEventGenerators)
	fmt.Fprintf(os.Stderr, "\tRateShape            : %v\n", generatorConfig.Configuration.RateShape)
	fmt.Fprintf(os.Stderr, "\tFirstEventRate       : %v\n", generatorConfig.Configuration.FirstEventRate)
	fmt.Fprintf(os.Stderr, "\tNextEventRate        : %v\n", generatorConfig.Configuration.NextEventRate)
	fmt.Fprintf(os.Stderr, "\tRateUnit             : %v\n", generatorConfig.Configuration.RateUnit)
	fmt.Fprintf(os.Stderr, "\tRatePeriodSec        : %v\n", generatorConfig.Configuration.RatePeriodSec)
	fmt.Fprintf(os.Stderr, "\tPreloadSeconds       : %v\n", generatorConfig.Configuration.PreloadSeconds)
	fmt.Fprintf(os.Stderr, "\tStreamTimeout        : %v\n", generatorConfig.Configuration.StreamTimeout)
	fmt.Fprintf(os.Stderr, "\tIsRateLimited        : %v\n", generatorConfig.Configuration.IsRateLimited)
	fmt.Fprintf(os.Stderr, "\tUseWallclockEventTime: %v\n", generatorConfig.Configuration.UseWallclockEventTime)
	fmt.Fprintf(os.Stderr, "\tAvgPersonByteSize    : %v\n", generatorConfig.Configuration.AvgPersonByteSize)
	fmt.Fprintf(os.Stderr, "\tAvgAuctionByteSize   : %v\n", generatorConfig.Configuration.AvgAuctionByteSize)
	fmt.Fprintf(os.Stderr, "\tAvgBidByteSize       : %v\n", generatorConfig.Configuration.AvgBidByteSize)
	fmt.Fprintf(os.Stderr, "\tHotAuctionRatio      : %v\n", generatorConfig.Configuration.HotAuctionRatio)
	fmt.Fprintf(os.Stderr, "\tHotSellersRatio      : %v\n", generatorConfig.Configuration.HotSellersRatio)
	fmt.Fprintf(os.Stderr, "\tHotBiddersRatio      : %v\n", generatorConfig.Configuration.HotBiddersRatio)
	fmt.Fprintf(os.Stderr, "\tWindowSizeSec        : %v\n", generatorConfig.Configuration.WindowSizeSec)
	fmt.Fprintf(os.Stderr, "\tWindowPeriodSec      : %v\n", generatorConfig.Configuration.WindowPeriodSec)
	fmt.Fprintf(os.Stderr, "\tWatermarkHoldbackSec : %v\n", generatorConfig.Configuration.WatermarkHoldbackSec)
	fmt.Fprintf(os.Stderr, "\tNumInFlightAuctions  : %v\n", generatorConfig.Configuration.NumInFlightAuctions)
	fmt.Fprintf(os.Stderr, "\tNumActivePeople      : %v\n", generatorConfig.Configuration.NumActivePeople)
	fmt.Fprintf(os.Stderr, "\tOccasionalDelaySec   : %v\n", generatorConfig.Configuration.OccasionalDelaySec)
	fmt.Fprintf(os.Stderr, "\tProbDelayedEvent     : %v\n", generatorConfig.Configuration.ProbDelayedEvent)
	fmt.Fprintf(os.Stderr, "\tOutOfOrderGroupSize  : %v\n", generatorConfig.Configuration.OutOfOrderGroupSize)
	eventGenerator := generator.NewSimpleNexmarkGenerator(generatorConfig, int(inputConfig.ParNum))
	duration := time.Duration(inputConfig.Duration) * time.Second
	eventSerde, txnMarkerSerde, msgSerde, err := h.getSerde(inputConfig.SerdeFormat)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}

	/*
		inChans := make([]chan sinkMsg, 0)
		g, ectx := errgroup.WithContext(ctx)
		for i := uint8(0); i < stream.NumPartition(); i++ {
			inChans = append(inChans, make(chan sinkMsg, 2))
			idxi := i
			g.Go(func() error {
				for sinkMsg := range inChans[idxi] {
					_, err = stream.Push(ectx, sinkMsg.encoded, sinkMsg.parNum, false)
					if err != nil {
						return err
					}
				}
				return nil
			})
		}
	*/

	cmm, err := transaction.NewControlChannelManager(h.env, inputConfig.AppId,
		commtypes.SerdeFormat(inputConfig.SerdeFormat), 0)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: err.Error(),
		}
	}
	controlErrc := make(chan error)
	controlQuit := make(chan struct{})
	meta := make(chan txn_data.ControlMetadata)
	dctx, dcancel := context.WithCancel(ctx)
	go cmm.MonitorControlChannel(ctx, controlQuit, controlErrc, meta)

	msgChan := make(chan sharedlog_stream.PayloadToPush, 100000)
	msgErrChan := make(chan error)
	var wg sync.WaitGroup
	flushMsgChan := func() {
		for len(msgChan) > 0 {
			time.Sleep(time.Duration(1) * time.Millisecond)
		}
	}
	procArgs := &nexmarkSrcProcArgs{
		channel_url_cache: make(map[uint32]*generator.ChannelUrl),
		eventSerde:        eventSerde,
		msgSerde:          msgSerde,
		eventGenerator:    eventGenerator,
		// inChans:           inChans,
		idx: 0,
		// errg:      g,
		latencies:    make([]int, 0, 128),
		msgChan:      msgChan,
		numPartition: stream.NumPartition(),
	}
	streamPusher := sharedlog_stream.StreamPush{
		MsgChan:    msgChan,
		MsgErrChan: msgErrChan,
		Stream:     stream,
		BufPush:    h.bufPush,
	}
	wg.Add(1)
	go streamPusher.AsyncStreamPush(ctx, &wg)
	streamPusher.FlushTimer = time.NewTicker(FLUSH_DURATION)
	startTime := time.Now()
	for {
		select {
		case merr := <-msgErrChan:
			controlQuit <- struct{}{}
			dcancel()
			return &common.FnOutput{Success: false, Message: fmt.Sprintf("failed to push to src stream: %v", merr)}
		case cerr := <-controlErrc:
			controlQuit <- struct{}{}
			if cerr != nil {
				/*
					if out := closeAllChanAndWait(inChans, g, cerr); out != nil {
						return out
					}
				*/
				close(msgChan)
				wg.Wait()
				if h.bufPush {
					err = stream.Flush(ctx)
					if err != nil {
						fmt.Fprintf(os.Stderr, "[Error] Flush failed: %v\n", err)
					}
				}
				dcancel()
				return &common.FnOutput{Success: false, Message: fmt.Sprintf("control channel manager failed: %v", cerr)}
			}
		case m := <-meta:
			numInstance := m.Config[h.funcName]
			if inputConfig.ParNum >= numInstance {
				controlQuit <- struct{}{}
				close(msgChan)
				wg.Wait()
				dcancel()
				return &common.FnOutput{
					Success:   true,
					Duration:  time.Since(startTime).Seconds(),
					Latencies: map[string][]int{"e2e": procArgs.latencies},
				}
			}
			numSubstreams := m.Config[stream.TopicName()]
			flushMsgChan()
			err = stream.ScaleSubstreams(h.env, numSubstreams)
			if err != nil {
				dcancel()
				return &common.FnOutput{Success: false, Message: err.Error()}
			}
			// piggy back scale fence in txn marker
			txnMarker := txn_data.TxnMarker{
				Mark:               uint8(txn_data.SCALE_FENCE),
				TranIDOrScaleEpoch: m.Epoch,
			}
			vBytes, err := txnMarkerSerde.Encode(txnMarker)
			if err != nil {
				dcancel()
				return &common.FnOutput{Success: false, Message: err.Error()}
			}
			encoded, err := msgSerde.Encode(nil, vBytes)
			if err != nil {
				dcancel()
				return &common.FnOutput{Success: false, Message: err.Error()}
			}
			var partitions []uint8
			for i := uint8(0); i < numSubstreams; i++ {
				partitions = append(partitions, i)
			}
			procArgs.numPartition = stream.NumPartition()
			msgChan <- sharedlog_stream.PayloadToPush{Payload: encoded, IsControl: true, Partitions: partitions}
			/*
				if h.bufPush {
					err = stream.Flush(ctx)
					if err != nil {
						dcancel()
						return &common.FnOutput{Success: false, Message: err.Error()}
					}
				}
				for i := uint8(0); i < numSubstreams; i++ {
					_, err = stream.Push(ctx, encoded, i, true, false)
					if err != nil {
						dcancel()
						return &common.FnOutput{Success: false, Message: err.Error()}
					}
				}
			*/
		default:
		}
		if !eventGenerator.HasNext() {
			break
		}
		if (duration != 0 && time.Since(startTime) >= duration) ||
			(inputConfig.EventsNum != 0 && procArgs.idx == int(inputConfig.EventsNum)) {
			break
		}
		fnout := h.process(dctx, procArgs)
		if fnout != nil && !fnout.Success {
			/*
				if out := closeAllChanAndWait(inChans, g, err); out != nil {
					return out
				}
			*/
			if h.bufPush {
				err = stream.Flush(ctx)
				if err != nil {
					fmt.Fprintf(os.Stderr, "[Error] Flush failed: %v\n", err)
				}
			}
			dcancel()
			return fnout
		}
	}
	/*
		if out := closeAllChanAndWait(inChans, g, err); out != nil {
			return out
		}
	*/
	close(msgChan)
	wg.Wait()
	if h.bufPush {
		err = stream.Flush(ctx)
		if err != nil {
			fmt.Fprintf(os.Stderr, "[Error] Flush failed: %v\n", err)
		}
	}
	dcancel()
	return &common.FnOutput{
		Success:   true,
		Duration:  time.Since(startTime).Seconds(),
		Latencies: map[string][]int{"e2e": procArgs.latencies},
	}
}
