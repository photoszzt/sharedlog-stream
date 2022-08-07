package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/benchmark/nexmark/pkg/nexmark/utils"
	"sharedlog-stream/pkg/commtypes"
	"time"

	"github.com/rs/zerolog/log"
)

var (
	FLAGS_faas_gateway string
	FLAGS_dump_spec    string
	FLAGS_dump_dir     string
	FLAGS_serdeFormat  string
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

func main() {
	flag.StringVar(&FLAGS_faas_gateway, "faas_gateway", "127.0.0.1:8081", "")
	flag.StringVar(&FLAGS_dump_dir, "dumpdir", "", "output dir for dumps")
	flag.StringVar(&FLAGS_dump_spec, "dumpspec", "", "dump spec")
	flag.StringVar(&FLAGS_serdeFormat, "serde", "json", "serde format: json or msgp")
	flag.Parse()

	if FLAGS_dump_dir == "" || FLAGS_dump_spec == "" {
		flag.Usage()
		return
	}
	serdeFormat := getSerdeFormat(FLAGS_serdeFormat)
	dumpInput := common.DumpStreams{
		DumpDir:     FLAGS_dump_dir,
		SerdeFormat: uint8(serdeFormat),
	}
	streamParamsBytes, err := os.ReadFile(FLAGS_dump_spec)
	if err != nil {
		panic(err)
	}
	streamParams := common.StreamParams{}
	err = json.Unmarshal(streamParamsBytes, &streamParams)
	if err != nil {
		panic(err)
	}
	dumpInput.StreamParams = streamParams.StreamParams
	err = os.MkdirAll(FLAGS_dump_dir, 0750)
	if err != nil {
		panic(err)
	}
	client := &http.Client{
		Transport: &http.Transport{
			IdleConnTimeout: 30 * time.Second,
		},
		Timeout: time.Duration(10) * time.Second,
	}
	url := utils.BuildFunctionUrl(FLAGS_faas_gateway, "dump")
	fmt.Printf("func source url is %v\n", url)
	var response common.FnOutput
	if err := utils.JsonPostRequest(client, url, "", &dumpInput, &response); err != nil {
		log.Error().Msgf("dump request failed: %v", err)
	} else if !response.Success {
		log.Error().Msgf("dump request failed: %s", response.Message)
	}
	fmt.Fprintf(os.Stderr, "Dump invoke done\n")
}
