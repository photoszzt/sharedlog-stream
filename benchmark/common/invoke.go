package common

import (
	"fmt"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/Jeffail/gabs/v2"
)

type SrcInvokeConfig struct {
	TopicName       string
	NodeConstraint  string
	ScaleEpoch      uint64
	NumOutPartition uint8
	InstanceID      uint8
	NumSrcInstance  uint8
}

type InvokeFuncParam struct {
	ConfigFile     string
	StatDir        string
	GatewayUrl     string
	WarmupTime     int
	Local          bool
	WaitForEndMark bool
}

func Invoke(invokeParam InvokeFuncParam,
	baseQueryInput *QueryInput,
	invokeSourceFunc func(client *http.Client, srcInvokeConfig SrcInvokeConfig, response *FnOutput, wg *sync.WaitGroup, warmup bool),
) error {
	byteVal, err := os.ReadFile(invokeParam.ConfigFile)
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
		fmt.Fprintf(os.Stderr, "config: %+v\n", config)
		ninstance := uint8(config["NumInstance"].Data().(float64))
		funcName := config["funcName"].Data().(string)
		outputTopicNamesTmp := config["OutputTopicName"].Data().([]interface{})
		inputTopicNamesTmp, ok := config["InputTopicNames"]
		numSrcProducerTmp := config["NumSrcProducer"]
		scaleConfig[funcName] = ninstance
		funcNames = append(funcNames, funcName)
		nodeConstraint := config["NodeConstraint"].Data().(string)
		if invokeParam.Local {
			nodeConstraint = ""
		}

		if !ok {
			// this is source config
			numSrcInstance = ninstance
			srcTopicName = outputTopicNamesTmp[0].(string)
			srcNodeConstraint = nodeConstraint
		} else {
			inputTopicNamesIntf := inputTopicNamesTmp.Data().([]interface{})
			numSrcProducerIntf := numSrcProducerTmp.Data().([]interface{})
			numSrcProducer := make([]uint8, len(numSrcProducerIntf))
			inputTopicNames := make([]string, len(inputTopicNamesIntf))
			outputTopicNames := make([]string, len(outputTopicNamesTmp))
			numOutPartitions := make([]uint8, len(outputTopicNamesTmp))

			for i, v := range inputTopicNamesIntf {
				inputTopicNames[i] = v.(string)
			}
			for i, v := range numSrcProducerIntf {
				numSrcProducer[i] = uint8(v.(float64))
			}
			for i, v := range outputTopicNamesTmp {
				outputTopicNames[i] = v.(string)
				numOutPartitions[i] = uint8(streamParam[outputTopicNames[i]].Data().(float64))
			}
			nconfig := &ClientNodeConfig{
				FuncName:       funcName,
				GatewayUrl:     invokeParam.GatewayUrl,
				NumInstance:    ninstance,
				NodeConstraint: nodeConstraint,
			}
			inParams := make([]*QueryInput, ninstance)
			for i := uint8(0); i < ninstance; i++ {
				numInSubs := uint8(streamParam[inputTopicNames[0]].Data().(float64))
				inParams[i] = &QueryInput{
					Duration:             baseQueryInput.Duration,
					GuaranteeMth:         baseQueryInput.GuaranteeMth,
					CommitEveryMs:        baseQueryInput.CommitEveryMs,
					SerdeFormat:          baseQueryInput.SerdeFormat,
					AppId:                baseQueryInput.AppId,
					TableType:            baseQueryInput.TableType,
					MongoAddr:            baseQueryInput.MongoAddr,
					FlushMs:              baseQueryInput.FlushMs,
					WarmupS:              baseQueryInput.WarmupS,
					InputTopicNames:      inputTopicNames,
					OutputTopicNames:     outputTopicNames,
					NumSubstreamProducer: numSrcProducer,
					NumInPartition:       numInSubs,
					NumOutPartitions:     numOutPartitions,
					WaitForEndMark:       invokeParam.WaitForEndMark,
					ScaleEpoch:           1,
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
	InvokeConfigScale(client, &scaleConfigInput, invokeParam.GatewayUrl,
		&scaleResponse, "scale", invokeParam.Local)

	numSrcPartition := uint8(streamParam[srcTopicName].Data().(float64))

	fmt.Fprintf(os.Stderr, "src instance: %d\n", numSrcInstance)
	var wg sync.WaitGroup

	sourceOutput := make([]FnOutput, numSrcInstance)
	outputMap := make(map[string][]FnOutput)
	for _, node := range cliNodes {
		outputMap[node.Name()] = make([]FnOutput, len(inParamsMap[node.Name()]))
	}

	for i := uint8(0); i < numSrcInstance; i++ {
		wg.Add(1)
		idx := i
		srcInvokeConfig := SrcInvokeConfig{
			TopicName:       srcTopicName,
			NodeConstraint:  srcNodeConstraint,
			ScaleEpoch:      1,
			InstanceID:      idx,
			NumOutPartition: numSrcPartition,
			NumSrcInstance:  numSrcInstance,
		}
		go invokeSourceFunc(client, srcInvokeConfig, &sourceOutput[idx], &wg, false)
	}

	time.Sleep(time.Duration(1) * time.Millisecond)

	for _, node := range cliNodes {
		funcName := node.Name()
		param := inParamsMap[funcName]
		output := outputMap[funcName]
		for j := uint8(0); j < uint8(len(param)); j++ {
			wg.Add(1)
			idx := j
			param[idx].ParNum = idx
			param[idx].Duration = baseQueryInput.Duration
			go node.Invoke(client, &output[idx], &wg, param[idx])
		}
	}
	fmt.Fprintf(os.Stderr, "Waiting for all client to return\n")
	wg.Wait()

	srcNum := make(map[string]uint64)
	srcEndToEnd := float64(0)
	maxDuration := float64(0)
	for i := uint8(0); i < numSrcInstance; i++ {
		idx := i
		if sourceOutput[idx].Success {
			if maxDuration < sourceOutput[idx].Duration {
				maxDuration = sourceOutput[idx].Duration
			}
			ProcessThroughputLat(fmt.Sprintf("source-%d", idx),
				invokeParam.StatDir, sourceOutput[idx].Latencies, sourceOutput[idx].Counts,
				sourceOutput[idx].Duration, srcNum, &srcEndToEnd)
		} else {
			fmt.Fprintf(os.Stderr, "source-%d failed\n", idx)
		}
	}
	if len(srcNum) != 0 {
		for k, v := range srcNum {
			fmt.Fprintf(os.Stderr, "%s processed %v events, duration: %v, tp: %f\n", k, v, maxDuration, float64(v)/maxDuration)
		}
	}

	for _, node := range cliNodes {
		funcName := node.Name()
		output := outputMap[funcName]
		num := make(map[string]uint64)
		endToEnd := float64(0)
		maxDuration := float64(0)
		for j := uint8(0); j < uint8(len(output)); j++ {
			if output[j].Success {
				if maxDuration < output[j].Duration {
					maxDuration = output[j].Duration
				}
				ProcessThroughputLat(fmt.Sprintf("%s-%d", funcName, j),
					invokeParam.StatDir, output[j].Latencies, output[j].Counts, output[j].Duration,
					num, &endToEnd)
			} else {
				fmt.Fprintf(os.Stderr, "%s-%d failed, msg %s\n", funcName, j, output[j].Message)
			}
		}
		if len(num) != 0 {
			for k, v := range num {
				fmt.Fprintf(os.Stderr, "%s processed %v events, duration: %v\n", k, v, maxDuration)
			}
			fmt.Fprintf(os.Stderr, "\n")
		}
	}
	return nil
}
