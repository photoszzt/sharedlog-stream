package common

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/Jeffail/gabs/v2"
)

func Invoke(config_file string, gateway_url string,
	baseQueryInput *QueryInput,
	invokeSourceFunc func(client *http.Client, numOutPartition uint8, topicName string,
		response *FnOutput, wg *sync.WaitGroup),
) error {
	jsonFile, err := os.Open(config_file)
	if err != nil {
		return err
	}
	defer jsonFile.Close()

	byteVal, err := ioutil.ReadAll(jsonFile)
	if err != nil {
		return err
	}
	jsonParsed, err := gabs.ParseJSON(byteVal)
	if err != nil {
		return err
	}
	funcParam := jsonParsed.S("FuncParam").Children()
	streamParam := jsonParsed.S("StreamParam").ChildrenMap()

	var cliNodes []*ClientNode
	var numSrcInstance uint8
	var srcTopicName string
	inParamsMap := make(map[string][]*QueryInput)
	for _, child := range funcParam {
		config := child.ChildrenMap()
		fmt.Fprintf(os.Stderr, "config: %v\n", config)
		ninstance := uint8(config["NumInstance"].Data().(float64))
		funcName := config["funcName"].Data().(string)
		outputTopicName := config["OutputTopicName"].Data().(string)
		inputTopicNamesTmp, ok := config["InputTopicNames"]
		if !ok {
			// this is source config
			numSrcInstance = ninstance
			srcTopicName = outputTopicName
		} else {
			inputTopicNamesIntf := inputTopicNamesTmp.Data().([]interface{})
			inputTopicNames := make([]string, len(inputTopicNamesIntf))
			for i, v := range inputTopicNamesIntf {
				inputTopicNames[i] = fmt.Sprint(v)
			}
			nconfig := &ClientNodeConfig{
				FuncName:    funcName,
				GatewayUrl:  gateway_url,
				NumInstance: ninstance,
			}
			inParams := make([]*QueryInput, ninstance)
			for i := uint8(0); i < ninstance; i++ {
				numInSubs := uint8(streamParam[inputTopicNames[0]].Data().(float64))
				numOutSubs := uint8(streamParam[outputTopicName].Data().(float64))
				inParams[i] = &QueryInput{
					Duration:          baseQueryInput.Duration,
					EnableTransaction: baseQueryInput.EnableTransaction,
					CommitEveryMs:     baseQueryInput.CommitEveryMs,
					SerdeFormat:       baseQueryInput.SerdeFormat,
					InputTopicNames:   inputTopicNames,
					OutputTopicName:   outputTopicName,
					NumInPartition:    numInSubs,
					NumOutPartition:   numOutSubs,
				}
			}
			node := NewClientNode(nconfig)
			cliNodes = append(cliNodes, node)
			inParamsMap[funcName] = inParams
		}
	}

	client := &http.Client{
		Transport: &http.Transport{
			IdleConnTimeout: 30 * time.Second,
		},
		Timeout: time.Duration(baseQueryInput.Duration*3) * time.Second,
	}

	var wg sync.WaitGroup
	sourceOutput := make([]FnOutput, numSrcInstance)
	numSrcPartition := uint8(streamParam[srcTopicName].Data().(float64))
	outputMap := make(map[string][]FnOutput)
	for _, node := range cliNodes {
		outputMap[node.Name()] = make([]FnOutput, len(inParamsMap[node.Name()]))
	}

	fmt.Fprintf(os.Stderr, "src instance: %d\n", numSrcInstance)
	for i := uint8(0); i < numSrcInstance; i++ {
		wg.Add(1)
		idx := i
		go invokeSourceFunc(client, numSrcPartition, srcTopicName, &sourceOutput[idx], &wg)
	}

	for _, node := range cliNodes {
		funcName := node.Name()
		param := inParamsMap[funcName]
		output := outputMap[funcName]
		for j := uint8(0); j < uint8(len(param)); j++ {
			wg.Add(1)
			idx := j
			param[idx].ParNum = idx
			go node.Invoke(client, &output[idx], &wg, param[idx])
		}
	}
	wg.Wait()

	for i := uint8(0); i < numSrcInstance; i++ {
		idx := i
		if sourceOutput[idx].Success {
			ProcessThroughputLat(fmt.Sprintf("source-%d", idx),
				sourceOutput[idx].Latencies, sourceOutput[idx].Duration)
		}
	}

	for _, node := range cliNodes {
		funcName := node.Name()
		output := outputMap[funcName]
		for j := uint8(0); j < uint8(len(output)); j++ {
			if output[j].Success {
				ProcessThroughputLat(fmt.Sprintf("%s-%d", funcName, j),
					output[j].Latencies, output[j].Duration)
			}
		}
	}
	return nil
}
