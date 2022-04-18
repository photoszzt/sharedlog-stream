package main

import (
	"flag"
	"fmt"
	"net/http"
	"sync"
	"time"

	"sharedlog-stream/benchmark/common"
	ntypes "sharedlog-stream/benchmark/nexmark/pkg/nexmark/types"
	"sharedlog-stream/pkg/stream/processor/commtypes"

	"sharedlog-stream/benchmark/nexmark/pkg/nexmark/utils"

	"github.com/rs/zerolog/log"
)

var (
	FLAGS_faas_gateway       string
	FLAGS_app_name           string
	FLAGS_stream_prefix      string
	FLAGS_duration           int
	FLAGS_events_num         int
	FLAGS_tps                int
	FLAGS_serdeFormat        string
	FLAGS_workload_config    string
	FLAGS_tran               bool
	FLAGS_commit_everyMs     uint64
	FLAGS_commit_every_niter uint
	FLAGS_exit_after_ncomm   uint
	FLAGS_scale_config       string
	FLAGS_table_type         string
	FLAGS_mongo_addr         string
)

func invokeSourceFunc(client *http.Client, numOutPartition uint8, topicName string,
	response *common.FnOutput, wg *sync.WaitGroup,
) {
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
	nexmarkConfig := ntypes.NewNexMarkConfigInput(topicName, serdeFormat)
	if FLAGS_duration != 0 {
		nexmarkConfig.Duration = uint32(FLAGS_duration) + uint32((time.Duration(10) * time.Second).Seconds())
	} else {
		nexmarkConfig.Duration = uint32(FLAGS_duration)
	}
	nexmarkConfig.FirstEventRate = uint32(FLAGS_tps)
	nexmarkConfig.NextEventRate = uint32(FLAGS_tps)
	nexmarkConfig.EventsNum = uint64(FLAGS_events_num)
	nexmarkConfig.NumOutPartition = numOutPartition
	url := utils.BuildFunctionUrl(FLAGS_faas_gateway, "source")
	fmt.Printf("func source url is %v\n", url)
	if err := utils.JsonPostRequest(client, url, nexmarkConfig, response); err != nil {
		log.Error().Msgf("source request failed: %v", err)
	} else if !response.Success {
		log.Error().Msgf("source request failed: %s", response.Message)
	}
}

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
	flag.Uint64Var(&FLAGS_commit_everyMs, "comm_everyMS", 10, "commit a transaction every (ms)")
	flag.UintVar(&FLAGS_commit_every_niter, "comm_every_niter", 0, "commit a transaction every iter(for test)")
	flag.UintVar(&FLAGS_exit_after_ncomm, "exit_after_ncomm", 0, "exit after n commits(for test)")
	flag.StringVar(&FLAGS_table_type, "tab_type", "mem", "either \"mem\" or \"mongodb\"")
	flag.StringVar(&FLAGS_mongo_addr, "mongo_addr", "", "mongodb address")
	flag.Parse()

	switch FLAGS_app_name {
	case "q1", "q2", "q3", "q5", "q7", "q8", "windowedAvg":
		err := common.Invoke(FLAGS_workload_config, FLAGS_faas_gateway,
			NewQueryInput(uint8(getSerdeFormat())), invokeSourceFunc)
		if err != nil {
			panic(err)
		}
	case "scale":
		scale()
	default:
		panic("unrecognized app")
	}
}
