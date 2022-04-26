package main

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"os"
	"time"

	"sharedlog-stream/benchmark/common"
)

func scale(format string) {
	serdeFormat := common.StringToSerdeFormat(format)

	jsonFile, err := os.Open(FLAGS_scale_config)
	if err != nil {
		panic(err)
	}
	defer jsonFile.Close()

	byteVal, err := ioutil.ReadAll(jsonFile)
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
	common.InvokeConfigScale(client, &scaleConfig, FLAGS_faas_gateway, &response, "scale")
}
