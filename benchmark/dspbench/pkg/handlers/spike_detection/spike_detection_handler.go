package spike_detection

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/benchmark/nexmark/pkg/nexmark/utils"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/stream"
	"sharedlog-stream/pkg/stream/processor"
	"time"

	"cs.utexas.edu/zjia/faas/types"
	"github.com/gammazero/deque"
)

const (
	SPIKE_THRESHOLD = 0.03
)

type ValAndAvg struct {
	Val float64
	Avg float64
}

type SumAndHist struct {
	Sum     float64
	history *deque.Deque
}

type spikeDetectionHandler struct {
	env types.Environment
}

func spikeDetectionPredicate(msg processor.Message) (bool, error) {
	valAvg := msg.Value.(ValAndAvg)
	return math.Abs(valAvg.Val-valAvg.Avg) > SPIKE_THRESHOLD*valAvg.Avg, nil
}

func NewSpikeDetectionHandler(env types.Environment) types.FuncHandler {
	return &spikeDetectionHandler{
		env: env,
	}
}

func (h *spikeDetectionHandler) Call(ctx context.Context, input []byte) ([]byte, error) {
	parsedInput := &common.QueryInput{}
	err := json.Unmarshal(input, parsedInput)
	if err != nil {
		return nil, err
	}
	fmt.Println("spike detection after parse input")
	outputCh := make(chan *common.FnOutput)
	go SpikeDetection(ctx, h.env, parsedInput, outputCh)
	output := <-outputCh
	encodedOutput, err := json.Marshal(output)
	if err != nil {
		panic(err)
	}
	fmt.Printf("spike detection outputs %s\n", encodedOutput)
	return utils.CompressData(encodedOutput), nil
}

func SpikeDetection(ctx context.Context, env types.Environment,
	input *common.QueryInput, output chan *common.FnOutput) {
	inputStream, err := sharedlog_stream.NewSharedLogStream(ctx, env, input.InputTopicName)
	if err != nil {
		output <- &common.FnOutput{
			Success: false,
			Message: fmt.Sprintf("NewSharedlogStream for input stream failed: %v", err),
		}
		return
	}
	/*
		outputStream, err := sharedlog_stream.NewSharedLogStream(ctx, env, input.OutputTopicName)
		if err != nil {
			output <- &common.FnOutput{
				Success: false,
				Message: fmt.Sprintf("NewSharedlogStream for output stream failed: %v", err),
			}
			return
		}
	*/
	// var msgEncoder processor.MsgEncoder
	var msgSerde processor.MsgSerde
	var sdSerde processor.Serde
	if input.SerdeFormat == uint8(common.JSON) {
		sdSerde = SensorDataJSONSerde{}
		msgSerde = common.MessageSerializedJSONSerde{}
	} else if input.SerdeFormat == uint8(common.MSGP) {
		msgSerde = common.MessageSerializedMsgpSerde{}
		sdSerde = SensorDataMsgpSerde{}
	} else {
		output <- &common.FnOutput{
			Success: false,
			Message: fmt.Sprintf("serde format should be either json or msgp; but %v is given", input.SerdeFormat),
		}
	}
	movingAverageWindow := 1000
	builder := stream.NewStreamBuilder()
	builder.Source("spike-detection-src", sharedlog_stream.NewSharedLogStreamSource(inputStream,
		int(input.Duration), processor.StringDecoder{}, sdSerde, msgSerde)).
		GroupByKey(&stream.Grouped{KeySerde: processor.StringSerde{},
			ValueSerde: sdSerde, Name: "group-by-devid"}).
		Aggregate("moving-avg", processor.InitializerFunc(func() interface{} {
			return &SumAndHist{
				Sum:     0,
				history: deque.New(movingAverageWindow),
			}
		}), processor.AggregatorFunc(func(key interface{}, value interface{}, agg interface{}) interface{} {
			nextVal := value.(float64)
			aggVal := agg.(*SumAndHist)
			if aggVal.history.Len() > movingAverageWindow-1 {
				valToRemove := aggVal.history.PopFront().(float64)
				aggVal.Sum -= valToRemove
			}
			aggVal.history.PushBack(nextVal)
			aggVal.Sum += nextVal
			return ValAndAvg{
				Val: nextVal,
				Avg: aggVal.Sum / float64(aggVal.history.Len()),
			}
		})).Filter("get-spike", processor.PredicateFunc(spikeDetectionPredicate), "")
	tp, err_arrs := builder.Build()
	if err_arrs != nil {
		output <- &common.FnOutput{
			Success: false,
			Message: fmt.Sprintf("build stream failed: %v", err_arrs),
		}
	}
	pumps := make(map[processor.Node]processor.Pump)
	var srcPumps []processor.SourcePump
	nodes := processor.FlattenNodeTree(tp.Sources())
	processor.ReverseNodes(nodes)
	for _, node := range nodes {
		pipe := processor.NewPipe(processor.ResolvePumps(pumps, node.Children()))
		node.Processor().WithPipe(pipe)

		pump := processor.NewSyncPump(node, pipe)
		pumps[node] = pump
	}
	for source, node := range tp.Sources() {
		srcPump := processor.NewSourcePump(node.Name(), source,
			processor.ResolvePumps(pumps, node.Children()), func(err error) {
				log.Fatal(err.Error())
			})
		srcPumps = append(srcPumps, srcPump)
	}

	duration := time.Duration(input.Duration) * time.Second
	latencies := make([]int, 0, 128)
	startTime := time.Now()
	select {
	case <-time.After(duration):
		for _, srcPump := range srcPumps {
			srcPump.Stop()
			srcPump.Close()
		}
	}
	output <- &common.FnOutput{
		Success:   true,
		Duration:  time.Since(startTime).Seconds(),
		Latencies: latencies,
	}

}
