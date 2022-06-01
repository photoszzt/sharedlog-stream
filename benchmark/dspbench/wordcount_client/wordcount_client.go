package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/benchmark/common/benchutil"
	"sharedlog-stream/benchmark/nexmark/pkg/nexmark/utils"
	"sharedlog-stream/pkg/commtypes"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
)

var (
	FLAGS_faas_gateway     string
	FLAGS_duration         int
	FLAGS_events_num       int
	FLAGS_serdeFormat      string
	FLAGS_file_name        string
	FLAGS_tran             bool
	FLAGS_commit_every     uint64
	FLAGS_dump_file_folder string
	FLAGS_stat_dir         string
)

func invokeSourceFunc(client *http.Client, response *common.FnOutput, wg *sync.WaitGroup, serdeFormat commtypes.SerdeFormat) {
	defer wg.Done()

	sp := &common.SourceParam{
		TopicName:   "wc_src",
		Duration:    uint32(FLAGS_duration),
		SerdeFormat: uint8(serdeFormat),
		NumEvents:   uint32(FLAGS_events_num),
		FileName:    FLAGS_file_name,
	}
	url := utils.BuildFunctionUrl(FLAGS_faas_gateway, "wcsource")
	if err := utils.JsonPostRequest(client, url, "", sp, response); err != nil {
		log.Error().Msgf("wcsource request failed: %v", err)
	} else if !response.Success {
		log.Error().Msgf("wcsource request failed: %s", response.Message)
	}
}

func main() {
	flag.StringVar(&FLAGS_faas_gateway, "faas_gateway", "127.0.0.1:8081", "")
	flag.IntVar(&FLAGS_duration, "duration", 60, "")
	flag.IntVar(&FLAGS_events_num, "nevent", 0, "number of events")
	flag.StringVar(&FLAGS_serdeFormat, "serde", "json", "serde format: json or msgp")
	flag.StringVar(&FLAGS_file_name, "in_fname", "/mnt/data/books.dat", "data source file name")
	flag.BoolVar(&FLAGS_tran, "tran", false, "enable transaction or not")
	flag.Uint64Var(&FLAGS_commit_every, "comm_every", 0, "commit a transaction every (ms)")
	flag.StringVar(&FLAGS_dump_file_folder, "dump_dir", "", "folder to dump the final output")
	flag.StringVar(&FLAGS_stat_dir, "stats_dir", "", "folder to store stats")
	flag.Parse()

	if FLAGS_stat_dir == "" {
		panic("should specify stats dir")
	}

	var serdeFormat commtypes.SerdeFormat

	if FLAGS_serdeFormat == "json" {
		serdeFormat = commtypes.JSON
	} else if FLAGS_serdeFormat == "msgp" {
		serdeFormat = commtypes.MSGP
	} else {
		log.Error().Msgf("serde format is not recognized; default back to JSON")
		serdeFormat = commtypes.JSON
	}

	numCountInstance := uint8(5)
	splitOutputTopic := "split_out"
	countOutputTopic := "wc_out"

	splitNodeConfig := &common.ClientNodeConfig{
		FuncName:    "wordcountsplit",
		GatewayUrl:  FLAGS_faas_gateway,
		NumInstance: 1,
	}

	splitInputParams := make([]*common.QueryInput, splitNodeConfig.NumInstance)
	for i := 0; i < int(splitNodeConfig.NumInstance); i++ {
		splitInputParams[i] = &common.QueryInput{
			Duration:          uint32(FLAGS_duration),
			InputTopicNames:   []string{"wc_src"},
			OutputTopicNames:  []string{splitOutputTopic},
			SerdeFormat:       uint8(serdeFormat),
			NumInPartition:    1,
			NumOutPartitions:  []uint8{numCountInstance},
			EnableTransaction: FLAGS_tran,
			CommitEveryMs:     FLAGS_commit_every,
		}
	}
	split := common.NewClientNode(splitNodeConfig)

	countNodeConfig := &common.ClientNodeConfig{
		FuncName:    "wordcountcounter",
		GatewayUrl:  FLAGS_faas_gateway,
		NumInstance: numCountInstance,
	}
	countInputParams := make([]*common.QueryInput, countNodeConfig.NumInstance)

	testParams := make(map[string]bool)
	envs := os.Environ()
	for _, env := range envs {
		if strings.HasPrefix(env, "Fail") {
			envSp := strings.Split(env, "=")
			testParams[envSp[0]] = true
		}
	}
	if len(testParams) == 0 {
		testParams = nil
	}
	for i := 0; i < int(countNodeConfig.NumInstance); i++ {
		countInputParams[i] = &common.QueryInput{
			Duration:          uint32(FLAGS_duration),
			InputTopicNames:   []string{splitOutputTopic},
			OutputTopicNames:  []string{countOutputTopic},
			SerdeFormat:       uint8(serdeFormat),
			NumInPartition:    numCountInstance,
			NumOutPartitions:  []uint8{numCountInstance},
			EnableTransaction: FLAGS_tran,
			CommitEveryMs:     FLAGS_commit_every,
			TestParams:        testParams,
		}
	}
	count := common.NewClientNode(countNodeConfig)

	client := &http.Client{
		Transport: &http.Transport{
			IdleConnTimeout: 90 * time.Second,
		},
		Timeout: time.Duration(FLAGS_duration*3) * time.Second,
	}
	var wg sync.WaitGroup

	var sourceOutput common.FnOutput

	splitOutput := make([]common.FnOutput, splitNodeConfig.NumInstance)
	countOutput := make([]common.FnOutput, countNodeConfig.NumInstance)

	wg.Add(1)
	go invokeSourceFunc(client, &sourceOutput, &wg, serdeFormat)

	for i := 0; i < int(splitNodeConfig.NumInstance); i++ {
		wg.Add(1)
		splitInputParams[i].ParNum = 0
		go split.Invoke(client, &splitOutput[i], &wg, splitInputParams[i])
	}

	for i := 0; i < int(countNodeConfig.NumInstance); i++ {
		wg.Add(1)
		countInputParams[i].ParNum = uint8(i)
		go count.Invoke(client, &countOutput[i], &wg, countInputParams[i])
	}

	wg.Wait()
	srcNum := make(map[string]uint64)
	srcEndToEnd := float64(0)
	if sourceOutput.Success {
		common.ProcessThroughputLat("source", FLAGS_stat_dir, sourceOutput.Latencies, sourceOutput.Counts,
			sourceOutput.Duration, srcNum, &srcEndToEnd)
	}
	if len(srcNum) != 0 {
		fmt.Fprintf(os.Stderr, "source throughput %v (event/s)\n", float64(srcNum["e2e"])/srcEndToEnd)
	}

	splitNum := make(map[string]uint64)
	splitEndToEnd := float64(0)
	for i := 0; i < int(splitNodeConfig.NumInstance); i++ {
		if splitOutput[i].Success {
			common.ProcessThroughputLat(fmt.Sprintf("split %v", i), FLAGS_stat_dir, splitOutput[i].Latencies,
				splitOutput[i].Counts, splitOutput[i].Duration, splitNum, &splitEndToEnd)
		}
	}
	if len(splitNum) != 0 {
		fmt.Fprintf(os.Stderr, "split throughput %v (event/s)\n", float64(splitNum["src"])/splitEndToEnd)
	}

	countNum := make(map[string]uint64)
	countEndToEnd := float64(0)
	for i := 0; i < int(countNodeConfig.NumInstance); i++ {
		if countOutput[i].Success {
			common.ProcessThroughputLat(fmt.Sprintf("count %d", i), FLAGS_stat_dir, countOutput[i].Latencies,
				countOutput[i].Counts, countOutput[i].Duration, countNum, &countEndToEnd)
		}
	}
	if len(countNum) != 0 {
		fmt.Fprintf(os.Stderr, "count throughput %v (event/s)\n", float64(countNum["src"])/countEndToEnd)
	}

	if FLAGS_dump_file_folder != "" {
		var dumpOutput common.FnOutput
		wg.Add(1)
		go benchutil.InvokeFunc(client, &dumpOutput, &wg, &common.DumpInput{
			DumpDir:       FLAGS_dump_file_folder,
			TopicName:     countOutputTopic,
			NumPartitions: numCountInstance,
			SerdeFormat:   uint8(serdeFormat),
		}, "wordcountDump", FLAGS_faas_gateway)
		wg.Wait()
	}
}
