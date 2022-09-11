package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/benchmark/nexmark/pkg/nexmark/ntypes"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/exactly_once_intr"
	"sharedlog-stream/pkg/store"
	"sync"
	"time"
)

var (
	FLAGS_faas_gateway    string
	FLAGS_app_name        string
	FLAGS_durBeforeScale  int
	FLAGS_durAfterScale   int
	FLAGS_tps             int
	FLAGS_events_num      int
	FLAGS_serdeFormat     string
	FLAGS_workload_config string
	FLAGS_scale_config    string
	FLAGS_guarantee       string
	FLAGS_commit_everyMs  uint64
	FLAGS_snapshot_everyS uint
	FLAGS_stat_dir        string
	FLAGS_local           bool
	FLAGS_flush_ms        int
	FLAGS_src_flush_ms    int
	FLAGS_waitForEndMark  bool
)

func NewQueryInput(serdeFormat commtypes.SerdeFormat, duration uint32) *common.QueryInput {
	table_type := store.IN_MEM
	guarantee := exactly_once_intr.AT_LEAST_ONCE
	if FLAGS_guarantee == "2pc" {
		guarantee = exactly_once_intr.TWO_PHASE_COMMIT
	} else if FLAGS_guarantee == "epoch" {
		guarantee = exactly_once_intr.EPOCH_MARK
	}
	return &common.QueryInput{
		Duration:      duration,
		GuaranteeMth:  uint8(guarantee),
		CommitEveryMs: FLAGS_commit_everyMs,
		SerdeFormat:   uint8(serdeFormat),
		AppId:         FLAGS_app_name,
		TableType:     uint8(table_type),
		FlushMs:       uint32(FLAGS_flush_ms),
		WarmupS:       0,
		SnapEveryS:    uint32(FLAGS_snapshot_everyS),
	}
}

func main() {
	flag.StringVar(&FLAGS_faas_gateway, "faas_gateway", "127.0.0.1:8081", "")
	flag.StringVar(&FLAGS_app_name, "app_name", "q1", "")
	flag.StringVar(&FLAGS_serdeFormat, "serde", "json", "serde format: json or msgp")
	flag.StringVar(&FLAGS_workload_config, "wconfig", "./wconfig.json", "path to a json file that stores workload config")
	flag.StringVar(&FLAGS_scale_config, "scconfig", "./scconfig.json", "path to a json file that stores scale config")
	flag.StringVar(&FLAGS_stat_dir, "stat_dir", "", "stats dir to dump")

	flag.IntVar(&FLAGS_durBeforeScale, "durBF", 60, "duration before scale")
	flag.IntVar(&FLAGS_durAfterScale, "durAF", 60, "duration after scale")
	flag.IntVar(&FLAGS_tps, "tps", 10000000, "tps param for nexmark")
	flag.IntVar(&FLAGS_flush_ms, "flushms", 10, "flush the buffer every ms; for exactly once, please see commit_everyMs and commit_niter. They determine the flush interval. ")
	flag.IntVar(&FLAGS_src_flush_ms, "src_flushms", 5, "src flush ms")
	flag.IntVar(&FLAGS_events_num, "events_num", 1000000, "events num")
	flag.UintVar(&FLAGS_snapshot_everyS, "snapshot_everyS", 0, "snapshot every s")

	flag.Uint64Var(&FLAGS_commit_everyMs, "comm_everyMS", 10, "commit a transaction every (ms)")

	flag.StringVar(&FLAGS_guarantee, "guarantee", "alo", "alo(at least once), 2pc(two phase commit) or epoch(epoch marking)")
	flag.BoolVar(&FLAGS_local, "local", false, "local mode without setting node constraint")
	flag.BoolVar(&FLAGS_waitForEndMark, "waitForLast", false, "wait for the final mark of input; used in measuring throughput")

	flag.Parse()
	if FLAGS_guarantee != "alo" && FLAGS_guarantee != "2pc" && FLAGS_guarantee != "epoch" {
		fmt.Fprintf(os.Stderr, "expected guarantee is alo, 2pc and epoch")
		return
	}
	serdeFormat := common.StringToSerdeFormat(FLAGS_serdeFormat)
	invokeFuncParam := common.InvokeFuncParam{
		ConfigFile:     FLAGS_workload_config,
		StatDir:        FLAGS_stat_dir,
		GatewayUrl:     FLAGS_faas_gateway,
		WarmupTime:     0,
		Local:          FLAGS_local,
		WaitForEndMark: FLAGS_waitForEndMark,
	}
	totTime := FLAGS_durBeforeScale + FLAGS_durAfterScale
	baseQueryInput := NewQueryInput(serdeFormat, uint32(FLAGS_durBeforeScale))
	srcInvokeConfig, cliNodes, inParamsMap, configScaleInput, err := common.ParseInvokeParam(
		invokeFuncParam, baseQueryInput)
	if err != nil {
		panic(err)
	}

	timeout := time.Duration(totTime*4) * time.Second
	if baseQueryInput.Duration == 0 {
		timeout = time.Duration(900) * time.Second
	}
	client := &http.Client{
		Transport: &http.Transport{
			IdleConnTimeout: 30 * time.Second,
		},
		Timeout: timeout,
	}
	scaleEpoch := uint16(1)
	configScaleInputInit := configScaleInput.Clone()
	configScaleInputInit.Bootstrap = true
	configScaleInputInit.ScaleEpoch = scaleEpoch
	var scaleResponse common.FnOutput
	common.InvokeConfigScale(client, &configScaleInputInit, invokeFuncParam.GatewayUrl,
		&scaleResponse, "scale", invokeFuncParam.Local)

	var wg sync.WaitGroup
	gp := ntypes.GeneratorParams{
		EventsNum:      uint64(FLAGS_events_num),
		SerdeFormat:    serdeFormat,
		FaasGateway:    FLAGS_faas_gateway,
		Duration:       uint32(totTime),
		Tps:            uint32(FLAGS_tps),
		FlushMs:        uint32(FLAGS_src_flush_ms),
		WaitForEndMark: FLAGS_waitForEndMark,
	}
	srcOutput := common.InvokeSrc(&wg, client, srcInvokeConfig, gp.InvokeSourceFunc, scaleEpoch)
	beforeScaleOutput := common.InvokeFunctions(&wg, client, cliNodes, inParamsMap, scaleEpoch)

	time.Sleep(time.Duration(FLAGS_durBeforeScale) * time.Second)

	invokeFuncParamScale := common.InvokeFuncParam{
		ConfigFile:     FLAGS_scale_config,
		StatDir:        FLAGS_stat_dir,
		GatewayUrl:     FLAGS_faas_gateway,
		WarmupTime:     0,
		Local:          FLAGS_local,
		WaitForEndMark: FLAGS_waitForEndMark,
	}
	baseQueryInputForScale := NewQueryInput(serdeFormat, uint32(FLAGS_durAfterScale))
	_, cliNodesForScale, inParamsMapForScale, configScaleInput2, err := common.ParseInvokeParam(
		invokeFuncParamScale, baseQueryInputForScale)
	if err != nil {
		panic(err)
	}
	scaleEpoch += 1
	configScaleInput2.ScaleEpoch = scaleEpoch
	var scaleOut2 common.FnOutput
	common.InvokeConfigScale(client, &configScaleInput2, invokeFuncParam.GatewayUrl,
		&scaleOut2, "scale", invokeFuncParam.Local)
	afterScaleOutput := common.InvokeFunctions(&wg, client, cliNodesForScale, inParamsMapForScale, scaleEpoch)
	wg.Wait()
	common.ParseSrcOutput(srcOutput, FLAGS_stat_dir)
	common.ParseFunctionOutputs(beforeScaleOutput, FLAGS_stat_dir)
	fmt.Fprintf(os.Stderr, "after scale\n")
	common.ParseFunctionOutputs(afterScaleOutput, FLAGS_stat_dir)
}
