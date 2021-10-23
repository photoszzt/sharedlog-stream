package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"
	"sync"
	"time"

	"sharedlog-stream/benchmark/common"
	ntypes "sharedlog-stream/benchmark/nexmark/pkg/nexmark/types"
	"sharedlog-stream/pkg/stream/processor"
	"sharedlog-stream/pkg/stream/processor/commtypes"

	"sharedlog-stream/benchmark/nexmark/pkg/nexmark/utils"

	"github.com/rs/zerolog/log"
)

var (
	FLAGS_faas_gateway  string
	FLAGS_app_name      string
	FLAGS_stream_prefix string
	FLAGS_duration      int
	FLAGS_events_num    int
	FLAGS_tps           int
	FLAGS_serdeFormat   string
)

func invokeSourceFunc(client *http.Client, response *common.FnOutput, wg *sync.WaitGroup) {
	defer wg.Done()
	var serdeFormat commtypes.SerdeFormat
	if FLAGS_serdeFormat == "json" {
		serdeFormat = commtypes.JSON
	} else if FLAGS_serdeFormat == "msgp" {
		serdeFormat = commtypes.MSGP
	} else {
		log.Error().Msgf("serde format is not recognized; default back to JSON")
		serdeFormat = commtypes.JSON
	}
	nexmarkConfig := ntypes.NewNexMarkConfigInput(FLAGS_stream_prefix+"_src", serdeFormat)
	nexmarkConfig.Duration = uint32(FLAGS_duration)
	nexmarkConfig.FirstEventRate = uint32(FLAGS_tps)
	nexmarkConfig.NextEventRate = uint32(FLAGS_tps)
	nexmarkConfig.EventsNum = uint64(FLAGS_events_num)
	url := utils.BuildFunctionUrl(FLAGS_faas_gateway, "source")
	fmt.Printf("func source url is %v\n", url)
	if err := utils.JsonPostRequest(client, url, nexmarkConfig, response); err != nil {
		log.Error().Msgf("source request failed: %v", err)
	} else if !response.Success {
		log.Error().Msgf("source request failed: %s", response.Message)
	}
}

func invokeQuery(client *http.Client, response *common.FnOutput, wg *sync.WaitGroup) {
	defer wg.Done()
	queryInput := &ntypes.QueryInput{
		Duration:        uint32(FLAGS_duration),
		InputTopicName:  FLAGS_stream_prefix + "_src",
		OutputTopicName: FLAGS_stream_prefix + "_" + FLAGS_app_name + "_output",
	}
	url := utils.BuildFunctionUrl(FLAGS_faas_gateway, FLAGS_app_name)
	fmt.Printf("func url is %v\n", url)
	if err := utils.JsonPostRequest(client, url, queryInput, response); err != nil {
		log.Error().Msgf("%v request failed with json post request returned err: %v", FLAGS_app_name, err)
	} else if !response.Success {
		log.Error().Msgf("%v request failed with unsuccess error: %v", FLAGS_app_name, response.Message)
	}
	fmt.Fprintf(os.Stderr, "response is %v\n", response)
}

func generalQuery() {
	client := &http.Client{
		Transport: &http.Transport{
			IdleConnTimeout: 30 * time.Second,
		},
		Timeout: time.Duration(FLAGS_duration*2) * time.Second,
	}
	var wg sync.WaitGroup
	var sourceOutput, queryOutput common.FnOutput
	wg.Add(1)
	go invokeSourceFunc(client, &sourceOutput, &wg)
	wg.Add(1)
	go invokeQuery(client, &queryOutput, &wg)
	wg.Wait()
}

func windowedAvg() {
	var serdeFormat commtypes.SerdeFormat
	if FLAGS_serdeFormat == "json" {
		serdeFormat = commtypes.JSON
	} else if FLAGS_serdeFormat == "msgp" {
		serdeFormat = commtypes.MSGP
	} else {
		log.Error().Msgf("serde format is not recognized; default back to JSON")
		serdeFormat = commtypes.JSON
	}

	numAggInstance := uint8(5)
	groupByNodeConfig := &processor.ClientNodeConfig{
		FuncName:    "windowavggroupby",
		GatewayUrl:  FLAGS_faas_gateway,
		NumInstance: 1,
	}
	groupByInputParams := make([]*common.QueryInput, groupByNodeConfig.NumInstance)
	for i := 0; i < int(groupByNodeConfig.NumInstance); i++ {
		groupByInputParams[i] = &common.QueryInput{
			Duration:        uint32(FLAGS_duration),
			InputTopicName:  "nexmark_src",
			OutputTopicName: "grouped_bid",
			SerdeFormat:     uint8(serdeFormat),
			NumInPartition:  1,
			NumOutPartition: numAggInstance,
		}
	}
	bidGroupBy := processor.NewClientNode(groupByNodeConfig)

	avgNodeConfig := &processor.ClientNodeConfig{
		FuncName:    "windowavgagg",
		GatewayUrl:  FLAGS_faas_gateway,
		NumInstance: uint32(numAggInstance),
	}
	avgNodeInputParams := make([]*common.QueryInput, avgNodeConfig.NumInstance)
	for i := 0; i < int(avgNodeConfig.NumInstance); i++ {
		avgNodeInputParams[i] = &common.QueryInput{
			Duration:        uint32(FLAGS_duration),
			InputTopicName:  "grouped_bid",
			OutputTopicName: "windowed_avg",
			SerdeFormat:     uint8(serdeFormat),
			NumInPartition:  numAggInstance,
			NumOutPartition: numAggInstance,
		}
	}
	avg := processor.NewClientNode(avgNodeConfig)

	client := &http.Client{
		Transport: &http.Transport{
			IdleConnTimeout: 30 * time.Second,
		},
		Timeout: time.Duration(FLAGS_duration*2) * time.Second,
	}

	var wg sync.WaitGroup
	var sourceOutput common.FnOutput

	groupByOutput := make([]common.FnOutput, groupByNodeConfig.NumInstance)
	avgOutput := make([]common.FnOutput, avgNodeConfig.NumInstance)

	wg.Add(1)
	go invokeSourceFunc(client, &sourceOutput, &wg)

	for i := 0; i < int(groupByNodeConfig.NumInstance); i++ {
		wg.Add(1)
		groupByInputParams[i].ParNum = 0
		go bidGroupBy.Invoke(client, &groupByOutput[i], &wg, groupByInputParams[i])
	}

	for i := 0; i < int(avgNodeConfig.NumInstance); i++ {
		wg.Add(1)
		avgNodeInputParams[i].ParNum = uint8(i)
		go avg.Invoke(client, &avgOutput[i], &wg, avgNodeInputParams[i])
	}
	wg.Wait()
	if sourceOutput.Success {
		common.ProcessThroughputLat("source", sourceOutput.Latencies["e2e"], sourceOutput.Duration)
	}
	for i := 0; i < int(groupByNodeConfig.NumInstance); i++ {
		if groupByOutput[i].Success {
			common.ProcessThroughputLat(fmt.Sprintf("groupby %v", i), groupByOutput[i].Latencies["e2e"], groupByOutput[i].Duration)
		}
	}
	for i := 0; i < int(avgNodeConfig.NumInstance); i++ {
		if avgOutput[i].Success {
			common.ProcessThroughputLat(fmt.Sprintf("avg %v", i), avgOutput[i].Latencies["e2e"], avgOutput[i].Duration)
		}
	}
}

func main() {
	flag.StringVar(&FLAGS_faas_gateway, "faas_gateway", "127.0.0.1:8081", "")
	flag.StringVar(&FLAGS_app_name, "app_name", "query1", "")
	flag.StringVar(&FLAGS_stream_prefix, "stream_prefix", "nexmark", "")
	flag.IntVar(&FLAGS_duration, "duration", 60, "")
	flag.IntVar(&FLAGS_events_num, "events_num", 100000000, "events.num param for nexmark")
	flag.IntVar(&FLAGS_tps, "tps", 10000000, "tps param for nexmark")
	flag.StringVar(&FLAGS_serdeFormat, "serde", "json", "serde format: json or msgp")
	flag.Parse()

	switch FLAGS_app_name {
	case "windowedAvg":
		windowedAvg()
	default:
		generalQuery()
	}
}
