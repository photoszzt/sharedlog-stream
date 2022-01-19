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
	output := eventGeneration(ctx, h.env, inputConfig)
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

func closeAllChan(inChans []chan sinkMsg) {
	for _, inChan := range inChans {
		close(inChan)
	}
}

func eventGeneration(ctx context.Context, env types.Environment, inputConfig *ntypes.NexMarkConfigInput) *common.FnOutput {
	stream, err := sharedlog_stream.NewShardedSharedLogStream(env, inputConfig.TopicName, inputConfig.NumOutPartition)
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
	latencies := make([]int, 0, 128)
	channel_url_cache := make(map[uint32]*generator.ChannelUrl)
	duration := time.Duration(inputConfig.Duration) * time.Second
	startTime := time.Now()
	var eventEncoder commtypes.Encoder
	var msgEncoder commtypes.MsgEncoder
	if inputConfig.SerdeFormat == uint8(commtypes.JSON) {
		eventEncoder = ntypes.EventJSONSerde{}
		msgEncoder = commtypes.MessageSerializedJSONSerde{}
	} else if inputConfig.SerdeFormat == uint8(commtypes.MSGP) {
		eventEncoder = ntypes.EventMsgpSerde{}
		msgEncoder = commtypes.MessageSerializedMsgpSerde{}
	} else {
		return &common.FnOutput{
			Success: false,
			Message: fmt.Sprintf("serde format should be either json or msgp; but %v is given", inputConfig.SerdeFormat),
		}
	}

	idx := 0

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

	for {
		if !eventGenerator.HasNext() {
			break
		}
		if duration != 0 && time.Since(startTime) >= duration {
			break
		}
		now := time.Now().Unix()
		nextEvent, err := eventGenerator.NextEvent(ctx, channel_url_cache)
		if err != nil {
			closeAllChan(inChans)
			g.Wait()
			return &common.FnOutput{
				Success: false,
				Message: fmt.Sprintf("next event failed: %v", err),
			}
		}
		wtsSec := nextEvent.WallclockTimestamp / 1000.0
		if wtsSec > uint64(now) {
			fmt.Fprintf(os.Stderr, "sleep %v second to generate event\n", wtsSec-uint64(now))
			time.Sleep(time.Duration(wtsSec-uint64(now)) * time.Second)
		}
		// fmt.Fprintf(os.Stderr, "gen event with ts: %v\n", nextEvent.EventTimestamp)
		encoded, err := eventEncoder.Encode(nextEvent.Event)
		if err != nil {
			closeAllChan(inChans)
			g.Wait()
			return &common.FnOutput{
				Success: false,
				Message: fmt.Sprintf("event serialization failed: %v", err),
			}
		}
		msgEncoded, err := msgEncoder.Encode(nil, encoded)
		if err != nil {
			closeAllChan(inChans)
			g.Wait()
			return &common.FnOutput{
				Success: false,
				Message: fmt.Sprintf("msg serialization failed: %v", err),
			}
		}
		// fmt.Fprintf(os.Stderr, "msg: %v\n", string(msgEncoded))
		idx += 1
		idx = idx % int(inputConfig.NumOutPartition)
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
		inChan := inChans[idx]
		sinkMsg := sinkMsg{
			encoded: msgEncoded,
			parNum:  uint8(idx),
		}
		inChan <- sinkMsg
		elapsed := time.Since(pushStart)
		latencies = append(latencies, int(elapsed.Microseconds()))
	}
	closeAllChan(inChans)
	if err := g.Wait(); err != nil {
		return &common.FnOutput{
			Success: false,
			Message: fmt.Sprintf("fail to wait for goroutine: %v", err),
		}
	}
	return &common.FnOutput{
		Success:  true,
		Duration: time.Since(startTime).Seconds(),
		Latencies: map[string][]int{
			"e2e": latencies,
		},
	}
}
