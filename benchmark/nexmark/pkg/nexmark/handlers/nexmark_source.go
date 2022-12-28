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
	"sharedlog-stream/pkg/debug"
	"sharedlog-stream/pkg/optional"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/stats"
	"sharedlog-stream/pkg/utils"

	"sharedlog-stream/benchmark/nexmark/pkg/nexmark/ntypes"
	"sharedlog-stream/pkg/commtypes"

	"sharedlog-stream/benchmark/nexmark/pkg/nexmark/generator"

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
	return common.CompressData(encodedOutput), nil
}

type nexmarkSrcProcArgs struct {
	msgSerde          commtypes.MessageGSerdeG[string, *ntypes.Event]
	channel_url_cache map[uint32]*generator.ChannelUrl
	eventGenerator    *generator.NexmarkGenerator
	msgChan           chan sharedlog_stream.PayloadToPush
	parNumArr         []uint8
	latencies         stats.StatsCollector[int64]
	idx               int
}

func (h *nexmarkSourceHandler) process(ctx context.Context, args *nexmarkSrcProcArgs) *common.FnOutput {
	procStart := stats.TimerBegin()
	nextEvent, err := args.eventGenerator.NextEvent(ctx, args.channel_url_cache)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: fmt.Sprintf("next event failed: %v\n", err),
		}
	}

	// fmt.Fprintf(os.Stderr, "gen event with ts: %v\n", nextEvent.EventTimestamp)
	msg := commtypes.MessageG[string, *ntypes.Event]{
		Key:   optional.None[string](),
		Value: optional.Some(nextEvent.Event),
	}
	msgEncoded, err := args.msgSerde.Encode(msg)
	if err != nil {
		return &common.FnOutput{Success: false, Message: fmt.Sprintf("msg serialization failed: %v", err)}
	}
	// fmt.Fprintf(os.Stderr, "msg: %v\n", string(msgEncoded))
	args.idx += 1
	idx := args.idx % len(args.parNumArr)
	// parNum := args.idx
	parNum := args.parNumArr[idx]

	nowMs := time.Now().UnixMilli()
	wtsMs := nextEvent.WallclockTimestamp
	if wtsMs > nowMs {
		// fmt.Fprintf(os.Stderr, "sleep %v ms to generate event\n", wtsSec-now)
		time.Sleep(time.Duration(wtsMs-nowMs) * time.Millisecond)
	}
	args.msgChan <- sharedlog_stream.PayloadToPush{Payload: msgEncoded, Partitions: []uint8{parNum}, IsControl: false}
	elapsed := stats.Elapsed(procStart).Microseconds()
	args.latencies.AddSample(elapsed)
	return nil
}

func getSubstreamIdx(parNum uint8, numPartition uint8, numGenerator uint8) []uint8 {
	if numPartition > numGenerator {
		numSubStreamPerGen := numPartition / numGenerator
		substreamIdx := make([]uint8, 0, numSubStreamPerGen)
		startIdx := parNum * numSubStreamPerGen
		endIdx := startIdx + numSubStreamPerGen
		for i := startIdx; i < endIdx; i++ {
			substreamIdx = append(substreamIdx, i)
		}
		return substreamIdx
	} else {
		return []uint8{parNum % numPartition}
	}
}

func (h *nexmarkSourceHandler) eventGeneration(ctx context.Context, inputConfig *ntypes.NexMarkConfigInput) *common.FnOutput {
	stream, err := sharedlog_stream.NewSizableShardedSharedLogStream(h.env, inputConfig.TopicName, inputConfig.NumOutPartition,
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
	substreamIdxOut := getSubstreamIdx(inputConfig.ParNum, inputConfig.NumOutPartition, uint8(generatorConfig.Configuration.NumEventGenerators))
	fmt.Fprintf(os.Stderr, "generate events to %v\n", substreamIdxOut)
	eventGenerator := generator.NewSimpleNexmarkGenerator(generatorConfig, int(inputConfig.ParNum))
	duration := time.Duration(inputConfig.Duration) * time.Second
	debug.Fprintf(os.Stderr, "Events per gen: %v, duration: %v\n", eventsPerGen, duration)
	serdeFormat := commtypes.SerdeFormat(inputConfig.SerdeFormat)
	eventSerde, err := ntypes.GetEventSerdeG(serdeFormat)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	msgSerde, err := commtypes.GetMsgGSerdeG[string](serdeFormat, commtypes.StringSerdeG{}, eventSerde)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	epochMarkerSerde, err := commtypes.GetEpochMarkerSerdeG(serdeFormat)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	cmm, err := control_channel.NewControlChannelManager(h.env, inputConfig.AppId,
		commtypes.SerdeFormat(inputConfig.SerdeFormat), 0, inputConfig.ParNum)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: err.Error(),
		}
	}
	dctx, dcancel := context.WithCancel(ctx)
	defer dcancel()
	cmm.StartMonitorControlChannel(dctx)

	streamPusher := sharedlog_stream.NewStreamPush(stream)
	var wg sync.WaitGroup
	flushMsgChan := func() {
		for len(streamPusher.MsgChan) > 0 {
			time.Sleep(time.Duration(100) * time.Microsecond)
		}
	}
	procArgs := &nexmarkSrcProcArgs{
		channel_url_cache: make(map[uint32]*generator.ChannelUrl),
		msgSerde:          msgSerde,
		eventGenerator:    eventGenerator,
		idx:               0,
		latencies: stats.NewStatsCollector[int64](fmt.Sprintf("srcGen_%d", inputConfig.ParNum),
			stats.DEFAULT_COLLECT_DURATION),
		msgChan:   streamPusher.MsgChan,
		parNumArr: substreamIdxOut,
	}
	wg.Add(1)
	go streamPusher.AsyncStreamPush(dctx, &wg, commtypes.EmptyProducerId, func() {})
	streamPusher.InitFlushTimer(time.Duration(inputConfig.FlushMs) * time.Millisecond)
	startTime := time.Now()
	fmt.Fprintf(os.Stderr, "StartTs: %d\n", startTime.UnixMilli())
	for {
		select {
		case merr := <-streamPusher.MsgErrChan:
			cmm.SendQuit()
			dcancel()
			return &common.FnOutput{Success: false, Message: fmt.Sprintf("failed to push to src stream: %v", merr)}
		case out := <-cmm.OutputChan():
			if out.Valid() {
				m := out.Value()
				debug.Fprintf(os.Stderr, "got data from control channel: %+v\n", m)
				// don't need to send scale fence on the initial deployment
				if len(m.Config) != 0 && m.Epoch != 1 {
					numInstance := m.Config[h.funcName]
					if inputConfig.ParNum >= numInstance {
						fmt.Fprintf(os.Stderr, "funcName: %s, parNum %v >= numInstance %v, quit\n", h.funcName, inputConfig.ParNum, numInstance)
						cmm.SendQuit()
						close(streamPusher.MsgChan)
						wg.Wait()
						return &common.FnOutput{
							Success:  true,
							Duration: time.Since(startTime).Seconds(),
							Counts: map[string]uint64{
								"sink":      streamPusher.GetCount(),
								"sink_ctrl": streamPusher.NumCtrlMsgs(),
							},
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
					encoded, err := epochMarkerSerde.Encode(txnMarker)
					if err != nil {
						return &common.FnOutput{Success: false, Message: err.Error()}
					}
					procArgs.parNumArr = getSubstreamIdx(inputConfig.ParNum, numSubstreams,
						uint8(generatorConfig.Configuration.NumEventGenerators))
					debug.Fprintf(os.Stderr, "updated numPartition is %d, generate events to %v\n",
						numSubstreams, procArgs.parNumArr)
					streamPusher.MsgChan <- sharedlog_stream.PayloadToPush{Payload: encoded,
						IsControl: true, Partitions: []uint8{inputConfig.ParNum}, Mark: commtypes.SCALE_FENCE}
				}
			} else {
				cerr := out.Err()
				cmm.SendQuit()
				close(streamPusher.MsgChan)
				wg.Wait()
				h.flush(ctx, stream)
				return &common.FnOutput{Success: false, Message: fmt.Sprintf("control channel manager failed: %v", cerr)}
			}
		default:
		}
		if !eventGenerator.HasNext() {
			if inputConfig.WaitForEndMark {
				for _, par := range procArgs.parNumArr {
					ret_err := genEndMark(startTime, par, epochMarkerSerde, streamPusher)
					if ret_err != nil {
						return ret_err
					}
				}
			}
			break
		}
		if procArgs.idx == int(eventsPerGen) {
			if inputConfig.WaitForEndMark {
				for _, par := range procArgs.parNumArr {
					ret_err := genEndMark(startTime, par, epochMarkerSerde, streamPusher)
					if ret_err != nil {
						return ret_err
					}
				}
			}
			break
		} else if duration != 0 && time.Since(startTime) >= duration {
			if inputConfig.WaitForEndMark {
				for _, par := range procArgs.parNumArr {
					ret_err := genEndMark(startTime, par, epochMarkerSerde, streamPusher)
					if ret_err != nil {
						return ret_err
					}
				}
			}
			break
		}
		fnout := h.process(dctx, procArgs)
		if fnout != nil && !fnout.Success {
			h.flush(ctx, stream)
			return fnout
		}
	}
	close(streamPusher.MsgChan)
	wg.Wait()
	h.flush(ctx, stream)
	return &common.FnOutput{
		Success:  true,
		Duration: time.Since(startTime).Seconds(),
		Counts: map[string]uint64{
			"sink":      streamPusher.GetCount(),
			"sink_ctrl": streamPusher.NumCtrlMsgs(),
		},
	}
}

func genEndMark(startTime time.Time, parNum uint8,
	epochMarkerSerde commtypes.SerdeG[commtypes.EpochMarker],
	streamPusher *sharedlog_stream.StreamPush,
) *common.FnOutput {
	// debug.Fprintf(os.Stderr, "generator exhausted, stop\n")
	marker := commtypes.EpochMarker{
		Mark:      commtypes.STREAM_END,
		StartTime: startTime.UnixMilli(),
		ProdIndex: parNum,
	}
	encoded, err := epochMarkerSerde.Encode(marker)
	if err != nil {
		return common.GenErrFnOutput(err)
	}
	streamPusher.MsgChan <- sharedlog_stream.PayloadToPush{Payload: encoded,
		IsControl: true, Partitions: []uint8{parNum}, Mark: commtypes.STREAM_END}
	return nil
}

func (h *nexmarkSourceHandler) flush(ctx context.Context, stream *sharedlog_stream.SizableShardedSharedLogStream) {
	if h.bufPush {
		_, err := stream.Flush(ctx, commtypes.EmptyProducerId, func() {})
		if err != nil {
			fmt.Fprintf(os.Stderr, "[Error] Flush failed: %v\n", err)
		}
	}
}
