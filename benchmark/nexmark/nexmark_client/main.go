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
	"sharedlog-stream/pkg/commtypes"

	"sharedlog-stream/benchmark/nexmark/pkg/nexmark/utils"

	"github.com/rs/zerolog/log"
)

var (
	FLAGS_faas_gateway    string
	FLAGS_app_name        string
	FLAGS_duration        int
	FLAGS_events_num      int
	FLAGS_tps             int
	FLAGS_serdeFormat     string
	FLAGS_workload_config string
	FLAGS_guarantee       string
	FLAGS_commit_everyMs  uint64
	FLAGS_scale_config    string
	FLAGS_table_type      string
	FLAGS_mongo_addr      string
	FLAGS_stat_dir        string
	FLAGS_warmup_time     int
	FLAGS_local           bool
	FLAGS_flush_ms        int
	FLAGS_dump_dir        string
	FLAGS_test_src        string
	FLAGS_waitForEndMark  bool
)

func getSerdeFormat(fmtStr string) commtypes.SerdeFormat {
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

type invokeSource func(client *http.Client, srcInvokeConfig common.SrcInvokeConfig, response *common.FnOutput, wg *sync.WaitGroup, warmup bool)

func invokeSourceFunc(client *http.Client, srcInvokeConfig common.SrcInvokeConfig, response *common.FnOutput, wg *sync.WaitGroup, warmup bool,
) {
	defer wg.Done()
	serdeFormat := getSerdeFormat(FLAGS_serdeFormat)
	nexmarkConfig := ntypes.NewNexMarkConfigInput(srcInvokeConfig.TopicName, serdeFormat)
	nexmarkConfig.Duration = uint32(FLAGS_duration)
	nexmarkConfig.EventsNum = uint64(FLAGS_events_num)
	nexmarkConfig.FirstEventRate = uint32(FLAGS_tps)
	nexmarkConfig.NextEventRate = uint32(FLAGS_tps)
	nexmarkConfig.NumOutPartition = srcInvokeConfig.NumOutPartition
	nexmarkConfig.ParNum = srcInvokeConfig.InstanceID
	nexmarkConfig.NumSrcInstance = srcInvokeConfig.NumSrcInstance
	nexmarkConfig.FlushMs = uint32(FLAGS_flush_ms)
	nexmarkConfig.WaitForEndMark = FLAGS_waitForEndMark
	url := utils.BuildFunctionUrl(FLAGS_faas_gateway, "source")
	fmt.Printf("func source url is %v\n", url)
	if err := utils.JsonPostRequest(client, url, srcInvokeConfig.NodeConstraint, nexmarkConfig, response); err != nil {
		log.Error().Msgf("source request failed: %v", err)
	} else if !response.Success {
		log.Error().Msgf("source request failed: %s", response.Message)
	}
	fmt.Fprintf(os.Stderr, "source-%d invoke done\n", srcInvokeConfig.InstanceID)
}

func invokeTestSrcFunc(client *http.Client, srcInvokeConfig common.SrcInvokeConfig,
	response *common.FnOutput, wg *sync.WaitGroup, warmup bool,
) {
	defer wg.Done()
	serdeFormat := getSerdeFormat(FLAGS_serdeFormat)
	src := common.TestSourceInput{
		FileName:    FLAGS_test_src,
		SerdeFormat: uint8(serdeFormat),
	}
	url := utils.BuildFunctionUrl(FLAGS_faas_gateway, "testSrc")
	fmt.Printf("func source url is %v\n", url)
	if err := utils.JsonPostRequest(client, url, srcInvokeConfig.NodeConstraint, &src, response); err != nil {
		log.Error().Msgf("source request failed: %v", err)
	} else if !response.Success {
		log.Error().Msgf("source request failed: %s", response.Message)
	}
	fmt.Fprintf(os.Stderr, "source-%d invoke done\n", srcInvokeConfig.InstanceID)
}

func invokeDumpFunc(client *http.Client) {
	serdeFormat := getSerdeFormat(FLAGS_serdeFormat)
	dumpInput := common.DumpStreams{
		DumpDir:     FLAGS_dump_dir,
		SerdeFormat: uint8(serdeFormat),
	}
	streamParam := common.StreamParam{}
	streamParam.TopicName = fmt.Sprintf("%s_out", FLAGS_app_name)
	switch FLAGS_app_name {
	case "q1", "q2":
		streamParam.KeySerde = "String"
		streamParam.ValueSerde = "Event"
	case "q3":
		streamParam.KeySerde = "Uint64"
		streamParam.ValueSerde = "NameCityStateId"
	case "q4", "q6":
		streamParam.KeySerde = "Uint64"
		streamParam.ValueSerde = "Float64"
	case "q5":
		streamParam.KeySerde = "StartEndTime"
		streamParam.ValueSerde = "AuctionIdCntMax"
	case "q7":
		streamParam.KeySerde = "Uint64"
		streamParam.ValueSerde = "BidAndMax"
	case "q8":
		streamParam.KeySerde = "Uint64"
		streamParam.ValueSerde = "PersonTime"
	}
	dumpInput.StreamParams = []common.StreamParam{streamParam}
	url := utils.BuildFunctionUrl(FLAGS_faas_gateway, "dump")
	fmt.Printf("func source url is %v\n", url)
	var response common.FnOutput
	if err := utils.JsonPostRequest(client, url, "", &dumpInput, &response); err != nil {
		log.Error().Msgf("dump request failed: %v", err)
	} else if !response.Success {
		log.Error().Msgf("dump request failed: %s", response.Message)
	}
	fmt.Fprintf(os.Stderr, "%sDump invoke done\n", FLAGS_app_name)
}

func main() {
	flag.StringVar(&FLAGS_faas_gateway, "faas_gateway", "127.0.0.1:8081", "")
	flag.StringVar(&FLAGS_app_name, "app_name", "q1", "")
	flag.StringVar(&FLAGS_serdeFormat, "serde", "json", "serde format: json or msgp")
	flag.StringVar(&FLAGS_workload_config, "wconfig", "./wconfig.json", "path to a json file that stores workload config")
	flag.StringVar(&FLAGS_table_type, "tab_type", "mem", "either \"mem\" or \"mongodb\"")
	flag.StringVar(&FLAGS_mongo_addr, "mongo_addr", "", "mongodb address")
	flag.StringVar(&FLAGS_stat_dir, "stat_dir", "", "stats dir to dump")
	flag.StringVar(&FLAGS_dump_dir, "dumpdir", "", "output dir for dumps")
	flag.StringVar(&FLAGS_test_src, "testsrc", "", "test event source file")

	flag.IntVar(&FLAGS_duration, "duration", 60, "")
	flag.IntVar(&FLAGS_events_num, "events_num", 100000000, "events.num param for nexmark")
	flag.IntVar(&FLAGS_tps, "tps", 10000000, "tps param for nexmark")
	flag.IntVar(&FLAGS_warmup_time, "warmup_time", 0, "warmup time")
	flag.IntVar(&FLAGS_flush_ms, "flushms", 10, "flush the buffer every ms; for exactly once, please see commit_everyMs and commit_niter. They determine the flush interval. ")

	flag.Uint64Var(&FLAGS_commit_everyMs, "comm_everyMS", 10, "commit a transaction every (ms)")

	flag.StringVar(&FLAGS_guarantee, "guarantee", "alo", "alo(at least once), 2pc(two phase commit) or epoch(epoch marking)")
	flag.BoolVar(&FLAGS_local, "local", false, "local mode without setting node constraint")
	flag.BoolVar(&FLAGS_waitForEndMark, "waitForLast", false, "wait for the final mark of input; used in measuring throughput")

	flag.Parse()

	if FLAGS_stat_dir == "" {
		panic("should specify non empty stats dir")
	}
	if FLAGS_guarantee != "alo" && FLAGS_guarantee != "2pc" && FLAGS_guarantee != "epoch" {
		fmt.Fprintf(os.Stderr, "expected guarantee is alo, 2pc and epoch")
		return
	}
	fmt.Fprintf(os.Stderr, "wait for last: %v\n", FLAGS_waitForEndMark)
	switch FLAGS_app_name {
	case "q1", "q2", "q3", "q4", "q5", "q6", "q7", "q8", "windowedAvg":
		var invokeSourceFunc_ invokeSource
		if FLAGS_test_src != "" {
			invokeSourceFunc_ = invokeTestSrcFunc
		} else {
			invokeSourceFunc_ = invokeSourceFunc
		}
		invokeFuncParam := common.InvokeFuncParam{
			ConfigFile:     FLAGS_workload_config,
			StatDir:        FLAGS_stat_dir,
			GatewayUrl:     FLAGS_faas_gateway,
			WarmupTime:     FLAGS_warmup_time,
			Local:          FLAGS_local,
			WaitForEndMark: FLAGS_waitForEndMark,
		}
		err := common.Invoke(invokeFuncParam,
			NewQueryInput(uint8(common.StringToSerdeFormat(FLAGS_serdeFormat))), invokeSourceFunc_)
		if err != nil {
			panic(err)
		}
		if FLAGS_dump_dir != "" {
			err = os.MkdirAll(FLAGS_dump_dir, 0750)
			if err != nil {
				panic(err)
			}
			client := &http.Client{
				Transport: &http.Transport{
					IdleConnTimeout: 30 * time.Second,
				},
				Timeout: time.Duration(FLAGS_duration) * time.Second,
			}
			invokeDumpFunc(client)
		}
	case "scale":
		scale(FLAGS_serdeFormat, FLAGS_local)
	default:
		panic("unrecognized app")
	}
}
