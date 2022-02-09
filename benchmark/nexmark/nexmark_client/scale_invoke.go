package main

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"os"
	"sync"
	"time"

	"sharedlog-stream/benchmark/common"
	configscale "sharedlog-stream/benchmark/common/config_scale"
)

func scale() {
	serdeFormat := getSerdeFormat()

	jsonFile, err := os.Open(FLAGS_scale_config)
	if err != nil {
		panic(err)
	}
	defer jsonFile.Close()

	byteVal, err := ioutil.ReadAll(jsonFile)
	if err != nil {
		panic(err)
	}
	var scaleConfig ScaleConfig
	err = json.Unmarshal(byteVal, &scaleConfig)
	if err != nil {
		panic(err)
	}
	client := &http.Client{
		Transport: &http.Transport{
			IdleConnTimeout: 30 * time.Second,
		},
		Timeout: time.Duration(FLAGS_duration*2) * time.Second,
	}

	var wg sync.WaitGroup

	var response common.FnOutput
	go configscale.InvokeConfigScale(client, scaleConfig.Config,
		scaleConfig.AppId, FLAGS_faas_gateway, uint8(serdeFormat),
		&response, &wg)
	wg.Wait()
}
