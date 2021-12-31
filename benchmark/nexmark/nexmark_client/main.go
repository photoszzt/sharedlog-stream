package main

import (
	"flag"
	"fmt"
	"net/http"
	"sync"

	"sharedlog-stream/benchmark/common"
	ntypes "sharedlog-stream/benchmark/nexmark/pkg/nexmark/types"
	"sharedlog-stream/pkg/stream/processor/commtypes"

	"sharedlog-stream/benchmark/nexmark/pkg/nexmark/utils"

	"github.com/rs/zerolog/log"
)

var (
	FLAGS_faas_gateway    string
	FLAGS_app_name        string
	FLAGS_stream_prefix   string
	FLAGS_duration        int
	FLAGS_events_num      int
	FLAGS_tps             int
	FLAGS_serdeFormat     string
	FLAGS_workload_config string
	FLAGS_tran            bool
	FLAGS_commit_every    uint64
)

func invokeSourceFunc(client *http.Client, numOutPartition uint8, parNum uint8, response *common.FnOutput, wg *sync.WaitGroup) {
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
	nexmarkConfig.NumOutPartition = numOutPartition
	nexmarkConfig.ParNum = parNum
	url := utils.BuildFunctionUrl(FLAGS_faas_gateway, "source")
	fmt.Printf("func source url is %v\n", url)
	if err := utils.JsonPostRequest(client, url, nexmarkConfig, response); err != nil {
		log.Error().Msgf("source request failed: %v", err)
	} else if !response.Success {
		log.Error().Msgf("source request failed: %s", response.Message)
	}
}

/*
func invokeQuery(client *http.Client, response *common.FnOutput, wg *sync.WaitGroup) {
	defer wg.Done()
	queryInput := &common.QueryInput{
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
	go invokeSourceFunc(client, 0, &sourceOutput, &wg)
	wg.Add(1)
	go invokeQuery(client, &queryOutput, &wg)
	wg.Wait()
}
*/

func getSerdeFormat() commtypes.SerdeFormat {
	var serdeFormat commtypes.SerdeFormat
	if FLAGS_serdeFormat == "json" {
		serdeFormat = commtypes.JSON
	} else if FLAGS_serdeFormat == "msgp" {
		serdeFormat = commtypes.MSGP
	} else {
		log.Error().Msgf("serde format is not recognized; default back to JSON")
		serdeFormat = commtypes.JSON
	}
	return serdeFormat
}

func main() {
	flag.StringVar(&FLAGS_faas_gateway, "faas_gateway", "127.0.0.1:8081", "")
	flag.StringVar(&FLAGS_app_name, "app_name", "q1", "")
	flag.StringVar(&FLAGS_stream_prefix, "stream_prefix", "nexmark", "")
	flag.IntVar(&FLAGS_duration, "duration", 60, "")
	flag.IntVar(&FLAGS_events_num, "events_num", 100000000, "events.num param for nexmark")
	flag.IntVar(&FLAGS_tps, "tps", 10000000, "tps param for nexmark")
	flag.StringVar(&FLAGS_serdeFormat, "serde", "json", "serde format: json or msgp")
	flag.StringVar(&FLAGS_workload_config, "wconfig", "./wconfig.json", "path to a json file that stores workload config")
	flag.BoolVar(&FLAGS_tran, "tran", false, "enable transaction or not")
	flag.Uint64Var(&FLAGS_commit_every, "comm_every", 0, "commit a transaction every (ms)")
	flag.Parse()

	switch FLAGS_app_name {
	case "windowedAvg":
		windowedAvg()
	case "q1":
		query1()
	case "q5":
		query5()
	case "q7":
		query7()
	default:
		panic("unrecognized app")
	}
}
