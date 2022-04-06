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
	scaleConfig := make(map[string]uint8)
	inParamsMap := make(map[string][]*QueryInput)
	funcNames := make([]string, 0)
	for _, child := range funcParam {
		config := child.ChildrenMap()
		fmt.Fprintf(os.Stderr, "config: %v\n", config)
		ninstance := uint8(config["NumInstance"].Data().(float64))
		funcName := config["funcName"].Data().(string)
		outputTopicName := config["OutputTopicName"].Data().(string)
		inputTopicNamesTmp, ok := config["InputTopicNames"]
		scaleConfig[funcName] = ninstance
		funcNames = append(funcNames, funcName)

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
					CommitEveryNIter:  baseQueryInput.CommitEveryNIter,
					ExitAfterNCommit:  baseQueryInput.ExitAfterNCommit,
					SerdeFormat:       baseQueryInput.SerdeFormat,
					AppId:             baseQueryInput.AppId,
					TableType:         baseQueryInput.TableType,
					MongoAddr:         baseQueryInput.MongoAddr,
					InputTopicNames:   inputTopicNames,
					OutputTopicName:   outputTopicName,
					NumInPartition:    numInSubs,
					NumOutPartition:   numOutSubs,
					ScaleEpoch:        1,
				}
			}
			node := NewClientNode(nconfig)
			cliNodes = append(cliNodes, node)
			inParamsMap[funcName] = inParams
		}
	}
	for tp, subs := range streamParam {
		scaleConfig[tp] = uint8(subs.Data().(float64))
	}

	client := &http.Client{
		Transport: &http.Transport{
			IdleConnTimeout: 30 * time.Second,
		},
		Timeout: time.Duration(baseQueryInput.Duration*3) * time.Second,
	}

	scaleConfigInput := ConfigScaleInput{
		Config:      scaleConfig,
		FuncNames:   funcNames,
		AppId:       baseQueryInput.AppId,
		ScaleEpoch:  1,
		SerdeFormat: baseQueryInput.SerdeFormat,
		Bootstrap:   true,
	}
	var scaleResponse FnOutput
	InvokeConfigScale(client, &scaleConfigInput, gateway_url, &scaleResponse, "scale")

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

	srcNum := uint64(0)
	srcEndToEnd := float64(0)
	for i := uint8(0); i < numSrcInstance; i++ {
		idx := i
		if sourceOutput[idx].Success {
			ProcessThroughputLat(fmt.Sprintf("source-%d", idx),
				sourceOutput[idx].Latencies, sourceOutput[idx].Consumed,
				sourceOutput[idx].Duration, &srcNum, &srcEndToEnd)
		} else {
			fmt.Fprintf(os.Stderr, "source-%d failed\n", idx)
		}
	}
	if srcNum != 0 {
		fmt.Fprintf(os.Stderr, "source throughput %v (event/s)\n\n", float64(srcNum)/srcEndToEnd)
	}

	for _, node := range cliNodes {
		funcName := node.Name()
		output := outputMap[funcName]
		num := uint64(0)
		endToEnd := float64(0)
		for j := uint8(0); j < uint8(len(output)); j++ {
			if output[j].Success {
				ProcessThroughputLat(fmt.Sprintf("%s-%d", funcName, j),
					output[j].Latencies, output[j].Consumed, output[j].Duration,
					&num, &endToEnd)
			} else {
				fmt.Fprintf(os.Stderr, "%s-%d failed\n", funcName, j)
			}
		}
		if num != 0 {
			fmt.Fprintf(os.Stderr, "%s throughput %v (event/s)\n\n", funcName, float64(num)/endToEnd)
		}
	}
	return nil
}
