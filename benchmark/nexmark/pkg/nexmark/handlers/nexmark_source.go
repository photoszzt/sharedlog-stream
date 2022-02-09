package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/benchmark/nexmark/pkg/nexmark/utils"
	"sharedlog-stream/pkg/sharedlog_stream"

	ntypes "sharedlog-stream/benchmark/nexmark/pkg/nexmark/types"
	"sharedlog-stream/pkg/stream/processor/commtypes"

	"sharedlog-stream/benchmark/nexmark/pkg/nexmark/generator"

	"cs.utexas.edu/zjia/faas/types"
	"golang.org/x/sync/errgroup"
)

type nexmarkSourceHandler struct {
	env types.Environment
}

func NewNexmarkSource(env types.Environment) types.FuncHandler {
	return &nexmarkSourceHandler{
		env: env,
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

func (h *nexmarkSourceHandler) getSerde(serdeFormat uint8) (commtypes.Serde, commtypes.MsgSerde, error) {
	var eventSerde commtypes.Serde
	var msgSerde commtypes.MsgSerde
	if serdeFormat == uint8(commtypes.JSON) {
		eventSerde = ntypes.EventJSONSerde{}
		msgSerde = commtypes.MessageSerializedJSONSerde{}
	} else if serdeFormat == uint8(commtypes.MSGP) {
		eventSerde = ntypes.EventMsgpSerde{}
		msgSerde = commtypes.MessageSerializedMsgpSerde{}
	} else {
		return nil, nil, fmt.Errorf("serde format should be either json or msgp; but %v is given", serdeFormat)
	}
	return eventSerde, msgSerde, nil
}

type nexmarkSrcProcArgs struct {
	eventSerde        commtypes.Serde
	msgSerde          commtypes.MsgSerde
	channel_url_cache map[uint32]*generator.ChannelUrl
	eventGenerator    *generator.NexmarkGenerator
	errg              *errgroup.Group
	stream            *sharedlog_stream.ShardedSharedLogStream
	inChans           []chan sinkMsg
	latencies         []int
	idx               int
}

func (h *nexmarkSourceHandler) process(ctx context.Context, args *nexmarkSrcProcArgs) *common.FnOutput {
	now := time.Now().Unix()
	nextEvent, err := args.eventGenerator.NextEvent(ctx, args.channel_url_cache)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: fmt.Sprintf("next event failed: %v\n", err),
		}
	}
	wtsSec := nextEvent.WallclockTimestamp / 1000.0
	if wtsSec > uint64(now) {
		fmt.Fprintf(os.Stderr, "sleep %v second to generate event\n", wtsSec-uint64(now))
		time.Sleep(time.Duration(wtsSec-uint64(now)) * time.Second)
	}
	// fmt.Fprintf(os.Stderr, "gen event with ts: %v\n", nextEvent.EventTimestamp)
	msgEncoded, err := encodeEvent(nextEvent.Event, args.eventSerde, args.msgSerde)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: fmt.Sprintf("msg serialization failed: %v", err),
		}
	}
	// fmt.Fprintf(os.Stderr, "msg: %v\n", string(msgEncoded))
	args.idx += 1
	args.idx = args.idx % int(args.stream.NumPartition())
	pushStart := time.Now()
	/*
		_, err = stream.Push(ctx, msgEncoded, uint8(idx), false)
		if err != nil {
			return &common.FnOutput{
				Success: false,
				Message: fmt.Sprintf("stream push failed: %v", err),
			}, nil
		}
		// fmt.Fprintf(os.Stderr, "inserted to pos: 0x%x\n", pos)
	*/
	inChan := args.inChans[args.idx]
	sinkMsg := sinkMsg{
		encoded: msgEncoded,
		parNum:  uint8(args.idx),
	}
	inChan <- sinkMsg
	elapsed := time.Since(pushStart)
	args.latencies = append(args.latencies, int(elapsed.Microseconds()))
	return nil
}

func (h *nexmarkSourceHandler) eventGeneration(ctx context.Context, inputConfig *ntypes.NexMarkConfigInput) *common.FnOutput {
	stream, err := sharedlog_stream.NewShardedSharedLogStream(h.env, inputConfig.TopicName, inputConfig.NumOutPartition)
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
	generatorConfig := generator.NewGeneratorConfig(nexmarkConfig, uint64(time.Now().UnixMilli()), 1, uint64(nexmarkConfig.NumEvents), 1)
	eventGenerator := generator.NewSimpleNexmarkGenerator(generatorConfig)
	duration := time.Duration(inputConfig.Duration) * time.Second
	startTime := time.Now()
	eventSerde, msgSerde, err := h.getSerde(inputConfig.SerdeFormat)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: err.Error(),
		}
	}

	inChans := make([]chan sinkMsg, 0)
	g, ectx := errgroup.WithContext(ctx)
	for i := uint8(0); i < inputConfig.NumOutPartition; i++ {
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

	procArgs := nexmarkSrcProcArgs{
		channel_url_cache: make(map[uint32]*generator.ChannelUrl),
		eventSerde:        eventSerde,
		msgSerde:          msgSerde,
		eventGenerator:    eventGenerator,
		inChans:           inChans,
		idx:               0,
		errg:              g,
		latencies:         make([]int, 0, 128),
		stream:            stream,
	}

	cmm, err := sharedlog_stream.NewControlChannelManager(h.env, inputConfig.AppId,
		commtypes.SerdeFormat(inputConfig.SerdeFormat))
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: err.Error(),
		}
	}
	controlErrc := make(chan error)
	controlQuit := make(chan struct{})
	dctx, dcancel := context.WithCancel(ctx)
	go cmm.MonitorControlChannel(ctx, controlQuit, controlErrc, dcancel)

	for {
		select {
		case cerr := <-controlErrc:
			controlQuit <- struct{}{}
			if cerr != nil {
				if out := closeAllChanAndWait(inChans, g, cerr); out != nil {
					return out
				}
				return &common.FnOutput{
					Success: false,
					Message: fmt.Sprintf("control channel manager failed: %v", cerr),
				}
			}
		default:
		}
		if !eventGenerator.HasNext() {
			break
		}
		if duration != 0 && time.Since(startTime) >= duration {
			break
		}
		fnout := h.process(dctx, &procArgs)
		if !fnout.Success {
			if out := closeAllChanAndWait(inChans, g, err); out != nil {
				return out
			}
			return fnout
		}
	}
	if out := closeAllChanAndWait(inChans, g, err); out != nil {
		return out
	}
	return &common.FnOutput{
		Success:  true,
		Duration: time.Since(startTime).Seconds(),
		Latencies: map[string][]int{
			"e2e": procArgs.latencies,
		},
	}
}
