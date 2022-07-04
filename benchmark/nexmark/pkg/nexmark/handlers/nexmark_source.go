package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/pkg/control_channel"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/utils"

	ntypes "sharedlog-stream/benchmark/nexmark/pkg/nexmark/types"
	"sharedlog-stream/pkg/commtypes"

	"sharedlog-stream/benchmark/nexmark/pkg/nexmark/generator"
	nexmarkutils "sharedlog-stream/benchmark/nexmark/pkg/nexmark/utils"

	"cs.utexas.edu/zjia/faas/types"
)

type nexmarkSourceHandler struct {
	env      types.Environment
	funcName string
	bufPush  bool
}

func NewNexmarkSource(env types.Environment, funcName string) types.FuncHandler {
	return &nexmarkSourceHandler{
		env:      env,
		funcName: funcName,
		bufPush:  utils.CheckBufPush(),
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
	return nexmarkutils.CompressData(encodedOutput), nil
}

type nexmarkSrcProcArgs struct {
	msgSerde          commtypes.MessageSerde
	channel_url_cache map[uint32]*generator.ChannelUrl
	eventGenerator    *generator.NexmarkGenerator
	msgChan           chan sharedlog_stream.PayloadToPush
	latencies         []int
	idx               int
	numPartition      uint8
	parNum            uint8
}

func (h *nexmarkSourceHandler) process(ctx context.Context, args *nexmarkSrcProcArgs) *common.FnOutput {
	nextEvent, err := args.eventGenerator.NextEvent(ctx, args.channel_url_cache)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: fmt.Sprintf("next event failed: %v\n", err),
		}
	}

	// fmt.Fprintf(os.Stderr, "gen event with ts: %v\n", nextEvent.EventTimestamp)
	msg := commtypes.Message{
		Key:   nil,
		Value: nextEvent.Event,
	}
	msgEncoded, err := args.msgSerde.Encode(&msg)
	if err != nil {
		return &common.FnOutput{Success: false, Message: fmt.Sprintf("msg serialization failed: %v", err)}
	}
	// fmt.Fprintf(os.Stderr, "msg: %v\n", string(msgEncoded))
	args.idx += 1
	// parNum := args.idx

	nowT := time.Now()
	nowMs := nowT.UnixMilli()
	wtsMs := nextEvent.WallclockTimestamp
	if wtsMs > nowMs {
		// fmt.Fprintf(os.Stderr, "sleep %v ms to generate event\n", wtsSec-now)
		time.Sleep(time.Duration(wtsMs-nowMs) * time.Millisecond)
	}
	args.msgChan <- sharedlog_stream.PayloadToPush{Payload: msgEncoded, Partitions: []uint8{uint8(args.parNum)}, IsControl: false}
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
	eventsPerGen := inputConfig.EventsNum / uint64(generatorConfig.Configuration.NumEventGenerators)
	eventGenerator := generator.NewSimpleNexmarkGenerator(generatorConfig, int(inputConfig.ParNum))
	duration := time.Duration(inputConfig.Duration) * time.Second
	serdeFormat := commtypes.SerdeFormat(inputConfig.SerdeFormat)
	eventSerde, err := ntypes.GetEventSerde(serdeFormat)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	msgSerde, err := commtypes.GetMsgSerde(serdeFormat, commtypes.StringSerde{}, eventSerde)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	epochMarkerSerde, err := commtypes.GetEpochMarkerSerde(serdeFormat)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	txnMarkMsgSerde, err := commtypes.GetMsgSerde(serdeFormat, commtypes.StringSerde{}, epochMarkerSerde)
	if err != nil {
		return common.GenErrFnOutput(err)
	}
	cmm, err := control_channel.NewControlChannelManager(h.env, inputConfig.AppId,
		commtypes.SerdeFormat(inputConfig.SerdeFormat), 0)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: err.Error(),
		}
	}
	dctx, dcancel := context.WithCancel(ctx)
	defer dcancel()
	cmm.StartMonitorControlChannel(dctx)

	msgChan := make(chan sharedlog_stream.PayloadToPush, 10000)
	msgErrChan := make(chan error)
	var wg sync.WaitGroup
	flushMsgChan := func() {
		for len(msgChan) > 0 {
			time.Sleep(time.Duration(100) * time.Microsecond)
		}
	}
	procArgs := &nexmarkSrcProcArgs{
		channel_url_cache: make(map[uint32]*generator.ChannelUrl),
		msgSerde:          msgSerde,
		eventGenerator:    eventGenerator,
		// inChans:           inChans,
		idx: 0,
		// errg:      g,
		latencies:    make([]int, 0, 128),
		msgChan:      msgChan,
		numPartition: stream.NumPartition(),
		parNum:       inputConfig.ParNum % stream.NumPartition(),
	}
	streamPusher := sharedlog_stream.StreamPush{
		MsgChan:    msgChan,
		MsgErrChan: msgErrChan,
		Stream:     stream,
		BufPush:    h.bufPush,
	}
	wg.Add(1)
	go streamPusher.AsyncStreamPush(dctx, &wg, commtypes.EmptyProducerId)
	streamPusher.InitFlushTimer(time.Duration(inputConfig.FlushMs) * time.Millisecond)
	startTime := time.Now()
	for {
		select {
		case merr := <-msgErrChan:
			cmm.SendQuit()
			dcancel()
			return &common.FnOutput{Success: false, Message: fmt.Sprintf("failed to push to src stream: %v", merr)}
		case out := <-cmm.OutputChan():
			if out.Valid() {
				m := out.Value()
				numInstance := m.Config[h.funcName]
				if inputConfig.ParNum >= numInstance {
					cmm.SendQuit()
					close(msgChan)
					wg.Wait()
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
					return &common.FnOutput{Success: false, Message: err.Error()}
				}
				// piggy back scale fence in txn marker
				txnMarker := commtypes.EpochMarker{
					Mark:       commtypes.SCALE_FENCE,
					ScaleEpoch: m.Epoch,
				}
				msg := commtypes.Message{
					Key:   nil,
					Value: txnMarker,
				}
				encoded, err := txnMarkMsgSerde.Encode(&msg)
				if err != nil {
					return &common.FnOutput{Success: false, Message: err.Error()}
				}
				var partitions []uint8
				for i := uint8(0); i < numSubstreams; i++ {
					partitions = append(partitions, i)
				}
				procArgs.numPartition = stream.NumPartition()
				msgChan <- sharedlog_stream.PayloadToPush{Payload: encoded, IsControl: true, Partitions: partitions}
			} else {
				cerr := out.Err()
				cmm.SendQuit()
				close(msgChan)
				wg.Wait()
				h.flush(ctx, stream)
				return &common.FnOutput{Success: false, Message: fmt.Sprintf("control channel manager failed: %v", cerr)}
			}
		default:
		}
		if !eventGenerator.HasNext() {
			break
		}
		if (duration != 0 && time.Since(startTime) >= duration) ||
			(eventsPerGen != 0 && procArgs.idx == int(eventsPerGen)) {
			break
		}
		fnout := h.process(dctx, procArgs)
		if fnout != nil && !fnout.Success {
			h.flush(ctx, stream)
			return fnout
		}
	}
	close(msgChan)
	wg.Wait()
	h.flush(ctx, stream)
	return &common.FnOutput{
		Success:   true,
		Duration:  time.Since(startTime).Seconds(),
		Latencies: map[string][]int{"e2e": procArgs.latencies},
	}
}

func (h *nexmarkSourceHandler) flush(ctx context.Context, stream *sharedlog_stream.ShardedSharedLogStream) {
	if h.bufPush {
		err := stream.Flush(ctx, commtypes.EmptyProducerId)
		if err != nil {
			fmt.Fprintf(os.Stderr, "[Error] Flush failed: %v\n", err)
		}
	}
}
