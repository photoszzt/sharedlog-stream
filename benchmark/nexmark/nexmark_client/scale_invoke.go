package main

import (
	"encoding/json"
	"net/http"
	"os"
	"time"

	"sharedlog-stream/benchmark/common"
)

func scale(format string, local bool) {
	serdeFormat := common.StringToSerdeFormat(format)

	byteVal, err := os.ReadFile(FLAGS_scale_config)
	if err != nil {
		panic(err)
	}
	var scaleConfig common.ConfigScaleInput
	err = json.Unmarshal(byteVal, &scaleConfig)
	if err != nil {
		panic(err)
	}
	scaleConfig.Bootstrap = false
	client := &http.Client{
		Transport: &http.Transport{
			IdleConnTimeout: 30 * time.Second,
		},
		Timeout: time.Duration(FLAGS_duration*2) * time.Second,
	}
	var response common.FnOutput
	scaleConfig.SerdeFormat = uint8(serdeFormat)
	common.InvokeConfigScale(client, &scaleConfig, FLAGS_faas_gateway, &response, "scale", local)
}
