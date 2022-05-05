package common

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/Jeffail/gabs/v2"
)

func Invoke(config_file string, stat_dir string, gateway_url string,
	baseQueryInput *QueryInput,
	invokeSourceFunc func(client *http.Client, numOutPartition uint8, topicName string,
		nodeConstraint string,
		instanceId uint8, numSrcInstance uint8,
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
	var srcNodeConstraint string
	scaleConfig := make(map[string]uint8)
	inParamsMap := make(map[string][]*QueryInput)
	funcNames := make([]string, 0)
	for _, child := range funcParam {
		config := child.ChildrenMap()
		fmt.Fprintf(os.Stderr, "config: %v\n", config)
		ninstance := uint8(config["NumInstance"].Data().(float64))
		funcName := config["funcName"].Data().(string)
		outputTopicNamesTmp := config["OutputTopicName"].Data().([]interface{})
		inputTopicNamesTmp, ok := config["InputTopicNames"]
		scaleConfig[funcName] = ninstance
		funcNames = append(funcNames, funcName)
		nodeConstraint := config["NodeConstraint"].Data().(string)

		if !ok {
			// this is source config
			numSrcInstance = ninstance
			srcTopicName = outputTopicNamesTmp[0].(string)
			srcNodeConstraint = nodeConstraint
		} else {
			inputTopicNamesIntf := inputTopicNamesTmp.Data().([]interface{})
			inputTopicNames := make([]string, len(inputTopicNamesIntf))
			outputTopicNames := make([]string, len(outputTopicNamesTmp))
			numOutPartitions := make([]uint8, len(outputTopicNamesTmp))
			for i, v := range inputTopicNamesIntf {
				inputTopicNames[i] = v.(string)
			}
			for i, v := range outputTopicNamesTmp {
				outputTopicNames[i] = v.(string)
				numOutPartitions[i] = uint8(streamParam[outputTopicNames[i]].Data().(float64))
			}
			nconfig := &ClientNodeConfig{
				FuncName:       funcName,
				GatewayUrl:     gateway_url,
				NumInstance:    ninstance,
				NodeConstraint: nodeConstraint,
			}
			inParams := make([]*QueryInput, ninstance)
			for i := uint8(0); i < ninstance; i++ {
				numInSubs := uint8(streamParam[inputTopicNames[0]].Data().(float64))
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
					OutputTopicNames:  outputTopicNames,
					NumInPartition:    numInSubs,
					NumOutPartitions:  numOutPartitions,
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

	timeout := time.Duration(baseQueryInput.Duration*4) * time.Second
	if baseQueryInput.Duration == 0 {
		timeout = time.Duration(900) * time.Second
	}
	client := &http.Client{
		Transport: &http.Transport{
			IdleConnTimeout: 30 * time.Second,
		},
		Timeout: timeout,
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
		go invokeSourceFunc(client, numSrcPartition, srcTopicName, srcNodeConstraint, idx, numSrcInstance,
			&sourceOutput[idx], &wg)
	}

	time.Sleep(time.Duration(10) * time.Second)

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

	srcNum := make(map[string]uint64)
	srcEndToEnd := float64(0)
	for i := uint8(0); i < numSrcInstance; i++ {
		idx := i
		if sourceOutput[idx].Success {
			ProcessThroughputLat(fmt.Sprintf("source-%d", idx),
				stat_dir,
				sourceOutput[idx].Latencies, sourceOutput[idx].Consumed,
				sourceOutput[idx].Duration, srcNum, &srcEndToEnd)
		} else {
			fmt.Fprintf(os.Stderr, "source-%d failed\n", idx)
		}
	}
	if len(srcNum) != 0 {
		fmt.Fprintf(os.Stderr, "source outputs %v events, time %v s, throughput %v (event/s)\n\n",
			srcNum["e2e"], srcEndToEnd, float64(srcNum["e2e"])/srcEndToEnd)
	}

	for _, node := range cliNodes {
		funcName := node.Name()
		output := outputMap[funcName]
		num := make(map[string]uint64)
		endToEnd := float64(0)
		for j := uint8(0); j < uint8(len(output)); j++ {
			if output[j].Success {
				ProcessThroughputLat(fmt.Sprintf("%s-%d", funcName, j),
					stat_dir,
					output[j].Latencies, output[j].Consumed, output[j].Duration,
					num, &endToEnd)
			} else {
				fmt.Fprintf(os.Stderr, "%s-%d failed, msg %s\n", funcName, j, output[j].Message)
			}
		}
		if len(num) != 0 {
			if len(num) == 1 {
				fmt.Fprintf(os.Stderr, "%s processed %v events, time %v s, throughput %v (event/s)\n",
					funcName, num["src"], endToEnd, float64(num["src"])/endToEnd)
				for name, count := range num {
					if strings.Contains(name, "sink") || strings.Contains(name, "Sink") {
						fmt.Fprintf(os.Stderr, "outputs to %s %v events\n\n", name, count)
					}
				}
			} else {
				for k, v := range num {
					fmt.Fprintf(os.Stderr, "%s processed %v events, time %v s, throughput %v (event/s)\n",
						k, v, endToEnd, float64(v)/endToEnd)
				}
				fmt.Fprintf(os.Stderr, "%s outputs %v events\n", funcName, num["sink"])
				fmt.Fprintf(os.Stderr, "\n")
			}
		}
	}
	return nil
}
