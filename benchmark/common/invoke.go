package common

import (
	"fmt"
	"net/http"
	"os"
	"sharedlog-stream/pkg/commtypes"
	"sync"
	"time"

	"github.com/Jeffail/gabs/v2"
	"github.com/rs/zerolog/log"
)

type SrcInvokeConfig struct {
	FinalTpNames    []string
	TopicName       string
	AppId           string
	NodeConstraint  string
	ScaleEpoch      uint16
	NumOutPartition uint8
	InstanceID      uint8
	NumSrcInstance  uint8
}

func (c *SrcInvokeConfig) Clone() SrcInvokeConfig {
	return SrcInvokeConfig{
		TopicName:       c.TopicName,
		AppId:           c.AppId,
		NodeConstraint:  c.NodeConstraint,
		ScaleEpoch:      c.ScaleEpoch,
		NumOutPartition: c.NumOutPartition,
		InstanceID:      c.InstanceID,
		NumSrcInstance:  c.NumSrcInstance,
	}
}

type InvokeFuncParam struct {
	ConfigFile     string
	StatDir        string
	GatewayUrl     string
	WarmupTime     int
	Local          bool
	WaitForEndMark bool
}

func ParseInvokeParam(invokeParam InvokeFuncParam, baseQueryInput *QueryInput,
) (*SrcInvokeConfig, []*ClientNode, map[string][]*QueryInput, *ConfigScaleInput, error) {
	byteVal, err := os.ReadFile(invokeParam.ConfigFile)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	jsonParsed, err := gabs.ParseJSON(byteVal)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	funcParam := jsonParsed.S("FuncParam").Children()
	streamParam := jsonParsed.S("StreamParam").ChildrenMap()

	var cliNodes []*ClientNode
	var srcInvokeConfig SrcInvokeConfig
	scaleConfig := make(map[string]uint8)
	inParamsMap := make(map[string][]*QueryInput)
	funcNames := make([]string, 0)
	changelogTmp, hasChangelog := streamParam["changelog"]
	changelog := uint8(0)
	if hasChangelog {
		changelog = uint8(changelogTmp.Data().(float64))
	}
	for _, child := range funcParam {
		config := child.ChildrenMap()
		fmt.Fprintf(os.Stderr, "config: %+v\n", config)
		ninstance := uint8(config["NumInstance"].Data().(float64))
		funcName := config["funcName"].Data().(string)
		outputTopicNamesTmp := config["OutputTopicName"].Data().([]interface{})
		inputTopicNamesTmp, ok := config["InputTopicNames"]
		_, hasFinal := config["Final"]
		numSrcProducerTmp := config["NumSrcProducer"]
		scaleConfig[funcName] = ninstance
		funcNames = append(funcNames, funcName)
		nodeConstraint := config["NodeConstraint"].Data().(string)
		if invokeParam.Local {
			nodeConstraint = ""
		}

		if !ok {
			// this is source config
			srcInvokeConfig.NumSrcInstance = ninstance
			srcInvokeConfig.TopicName = outputTopicNamesTmp[0].(string)
			srcInvokeConfig.NodeConstraint = nodeConstraint
			srcInvokeConfig.AppId = baseQueryInput.AppId
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
			if hasFinal {
				srcInvokeConfig.FinalTpNames = outputTopicNames
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
				baseClone := baseQueryInput.Clone()
				baseClone.InputTopicNames = inputTopicNames
				baseClone.OutputTopicNames = outputTopicNames
				baseClone.NumSubstreamProducer = numSrcProducer
				baseClone.NumInPartition = numInSubs
				baseClone.NumOutPartitions = numOutPartitions
				baseClone.WaitForEndMark = invokeParam.WaitForEndMark
				if hasChangelog {
					baseClone.NumChangelogPartition = changelog
				}
				inParams[i] = &baseClone
			}
			node := NewClientNode(nconfig)
			cliNodes = append(cliNodes, node)
			inParamsMap[funcName] = inParams
		}
	}
	for tp, subs := range streamParam {
		scaleConfig[tp] = uint8(subs.Data().(float64))
	}
	srcInvokeConfig.NumOutPartition = uint8(streamParam[srcInvokeConfig.TopicName].Data().(float64))
	configScaleInput := ConfigScaleInput{
		Config:      scaleConfig,
		FuncNames:   funcNames,
		SerdeFormat: baseQueryInput.SerdeFormat,
		AppId:       baseQueryInput.AppId,
		BufMaxSize:  baseQueryInput.BufMaxSize,
	}
	return &srcInvokeConfig, cliNodes, inParamsMap, &configScaleInput, nil
}

func InvokeSrc(wg *sync.WaitGroup, client *http.Client,
	srcInvokeConfig *SrcInvokeConfig,
	invokeSourceFunc InvokeSrcFunc,
	scaleEpoch uint16,
) []FnOutput {
	sourceOutput := make([]FnOutput, srcInvokeConfig.NumSrcInstance)
	for i := uint8(0); i < srcInvokeConfig.NumSrcInstance; i++ {
		wg.Add(1)
		idx := i
		srcInvokeConfigCpy := srcInvokeConfig.Clone()
		srcInvokeConfigCpy.ScaleEpoch = scaleEpoch
		srcInvokeConfigCpy.InstanceID = idx
		go invokeSourceFunc(client, srcInvokeConfigCpy, &sourceOutput[idx], wg, false)
	}
	return sourceOutput
}

func InvokeFunctions(wg *sync.WaitGroup, client *http.Client, cliNodes []*ClientNode,
	inParamsMap map[string][]*QueryInput, scaleEpoch uint16,
) map[string][]FnOutput {
	outputMap := make(map[string][]FnOutput)
	for _, node := range cliNodes {
		outputMap[node.Name()] = make([]FnOutput, len(inParamsMap[node.Name()]))
	}
	for _, node := range cliNodes {
		funcName := node.Name()
		param := inParamsMap[funcName]
		output := outputMap[funcName]
		for j := uint8(0); j < uint8(len(param)); j++ {
			wg.Add(1)
			idx := j
			param[idx].ParNum = idx
			param[idx].Duration = param[0].Duration
			param[idx].ScaleEpoch = scaleEpoch
			go node.Invoke(client, &output[idx], wg, param[idx])
		}
	}
	return outputMap
}

func ParseSrcOutput(sourceOutput []FnOutput, statsDir string) {
	srcNum := make(map[string]uint64)
	srcEndToEnd := float64(0)
	maxDuration := float64(0)
	for idx, out := range sourceOutput {
		if out.Success {
			if maxDuration < sourceOutput[idx].Duration {
				maxDuration = sourceOutput[idx].Duration
			}
			ProcessThroughputLat(fmt.Sprintf("source-%d", idx),
				statsDir, sourceOutput[idx].Latencies, sourceOutput[idx].Counts,
				sourceOutput[idx].EventTs, sourceOutput[idx].Duration, srcNum, &srcEndToEnd)
		} else {
			fmt.Fprintf(os.Stderr, "source-%d failed\n", idx)
		}
	}
	if len(srcNum) != 0 {
		for k, v := range srcNum {
			fmt.Fprintf(os.Stderr, "%s processed %v events, duration: %v, tp: %f\n", k, v, maxDuration, float64(v)/maxDuration)
		}
		fmt.Fprintf(os.Stderr, "\n")
	}
}

func ParseFunctionOutputs(outputMap map[string][]FnOutput, statDir string) {
	for funcName, output := range outputMap {
		num := make(map[string]uint64)
		endToEnd := float64(0)
		maxDuration := float64(0)
		for j := uint8(0); j < uint8(len(output)); j++ {
			if output[j].Success {
				if maxDuration < output[j].Duration {
					maxDuration = output[j].Duration
				}
				ProcessThroughputLat(fmt.Sprintf("%s-%d", funcName, j),
					statDir, output[j].Latencies, output[j].Counts, output[j].EventTs, output[j].Duration,
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
}

type InvokeSrcFunc func(client *http.Client, srcInvokeConfig SrcInvokeConfig, response *FnOutput, wg *sync.WaitGroup, warmup bool)

func Invoke(invokeParam InvokeFuncParam,
	baseQueryInput *QueryInput,
	invokeSourceFunc InvokeSrcFunc,
) error {
	srcInvokeConfig, cliNodes, inParamsMap, configScaleInput, err := ParseInvokeParam(invokeParam, baseQueryInput)
	if err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "srcInvokeConfig: %+v\n", srcInvokeConfig)
	fmt.Fprintf(os.Stderr, "cliNodes: %+v\n", cliNodes)
	fmt.Fprintf(os.Stderr, "inParamsMap: %+v\n", inParamsMap)
	fmt.Fprintf(os.Stderr, "configScaleInput: %+v\n", configScaleInput)

	timeout := time.Duration(baseQueryInput.Duration+120) * time.Second
	if baseQueryInput.Duration == 0 {
		timeout = time.Duration(900) * time.Second
	}
	client := &http.Client{
		Transport: &http.Transport{
			IdleConnTimeout: 30 * time.Second,
		},
		Timeout: timeout,
	}
	configScaleInput.Bootstrap = true
	configScaleInput.ScaleEpoch = 1
	var scaleResponse FnOutput
	InvokeConfigScale(client, configScaleInput, invokeParam.GatewayUrl,
		&scaleResponse, "scale", invokeParam.Local)

	fmt.Fprintf(os.Stderr, "src instance: %d\n", srcInvokeConfig.NumSrcInstance)
	var wg sync.WaitGroup

	sourceOutput := InvokeSrc(&wg, client, srcInvokeConfig, invokeSourceFunc, 1)

	time.Sleep(time.Duration(1) * time.Millisecond)

	outputMap := InvokeFunctions(&wg, client, cliNodes, inParamsMap, 1)
	fmt.Fprintf(os.Stderr, "Waiting for all client to return\n")
	wg.Wait()

	ParseSrcOutput(sourceOutput, invokeParam.StatDir)
	ParseFunctionOutputs(outputMap, invokeParam.StatDir)
	return nil
}

func InvokeDumpFunc(client *http.Client, dumpDir string,
	app_name string, serdeFormat commtypes.SerdeFormat, faas_gateway string,
) {
	dumpInput := DumpStreams{
		DumpDir:     dumpDir,
		SerdeFormat: uint8(serdeFormat),
	}
	switch app_name {
	case "q1", "q2":
		dumpInput.StreamParams = []StreamParam{
			{
				TopicName:     fmt.Sprintf("%s_out", app_name),
				NumPartitions: 4,
				KeySerde:      "String",
				ValueSerde:    "Event",
			},
		}
	case "q3":
		dumpInput.StreamParams = []StreamParam{
			{
				TopicName:     fmt.Sprintf("%s_out", app_name),
				NumPartitions: 4,
				KeySerde:      "Uint64",
				ValueSerde:    "NameCityStateId",
			},
		}
	case "q4":
		dumpInput.StreamParams = []StreamParam{
			{
				TopicName:     fmt.Sprintf("%s_out", app_name),
				NumPartitions: 4,
				KeySerde:      "Uint64",
				ValueSerde:    "Float64",
			},
			{
				TopicName:     "q46_aucsByID",
				NumPartitions: 4,
				KeySerde:      "Uint64",
				ValueSerde:    "Event",
			},
			{
				TopicName:     "q46_bidsByAucID",
				NumPartitions: 4,
				KeySerde:      "Uint64",
				ValueSerde:    "Event",
			},
			{
				TopicName:     "q4_aucIDCat",
				NumPartitions: 4,
				KeySerde:      "AuctionIdCategory",
				ValueSerde:    "AuctionBid",
			},
			{
				TopicName:     "q4_maxBids",
				NumPartitions: 4,
				KeySerde:      "Uint64",
				ValueSerde:    "ChangeUint64",
			},
		}
	case "q6":
		dumpInput.StreamParams = []StreamParam{
			{
				TopicName:     fmt.Sprintf("%s_out", app_name),
				NumPartitions: 4,
				KeySerde:      "Uint64",
				ValueSerde:    "Float64",
			},
			{
				TopicName:     "q46_aucsByID",
				NumPartitions: 4,
				KeySerde:      "Uint64",
				ValueSerde:    "Event",
			},
			{
				TopicName:     "q46_bidsByAucID",
				NumPartitions: 4,
				KeySerde:      "Uint64",
				ValueSerde:    "Event",
			},
			{
				TopicName:     "q6_aucIDSeller",
				NumPartitions: 4,
				KeySerde:      "AuctionIdSeller",
				ValueSerde:    "AuctionBid",
			},
			{
				TopicName:     "q6_maxBids",
				NumPartitions: 4,
				KeySerde:      "Uint64",
				ValueSerde:    "ChangePriceTime",
			},
		}
	case "q5":
		dumpInput.StreamParams = []StreamParam{
			{
				TopicName:     fmt.Sprintf("%s_out", app_name),
				NumPartitions: 4,
				KeySerde:      "StartEndTime",
				ValueSerde:    "AuctionIdCntMax",
			},
			{
				TopicName:     "bids",
				NumPartitions: 4,
				KeySerde:      "Uint64",
				ValueSerde:    "Event",
			},
			{
				TopicName:     "aucBids",
				NumPartitions: 4,
				KeySerde:      "StartEndTime",
				ValueSerde:    "AuctionIdCount",
			},
		}
	case "q7":
		dumpInput.StreamParams = []StreamParam{
			{
				TopicName:     fmt.Sprintf("%s_out", app_name),
				NumPartitions: 4,
				KeySerde:      "Uint64",
				ValueSerde:    "BidAndMax",
			},
			{
				TopicName:     "bid_by_price",
				NumPartitions: 4,
				KeySerde:      "Uint64",
				ValueSerde:    "Event",
			},
			{
				TopicName:     "bid_by_win",
				NumPartitions: 4,
				KeySerde:      "StartEndTime",
				ValueSerde:    "Event",
			},
			{
				TopicName:     "max_bids",
				NumPartitions: 4,
				KeySerde:      "Uint64",
				ValueSerde:    "StartEndTime",
			},
		}
	case "q8":
		dumpInput.StreamParams = []StreamParam{
			{
				TopicName:     fmt.Sprintf("%s_out", app_name),
				NumPartitions: 4,
				KeySerde:      "Uint64",
				ValueSerde:    "PersonTime",
			},
			{
				TopicName:     "q8_aucsBySellerID_out",
				NumPartitions: 4,
				KeySerde:      "Uint64",
				ValueSerde:    "Event",
			},
			{
				TopicName:     "q8_personsByID_out",
				NumPartitions: 4,
				KeySerde:      "Uint64",
				ValueSerde:    "Event",
			},
		}
	}
	url := BuildFunctionUrl(faas_gateway, "dump")
	fmt.Printf("func source url is %v\n", url)
	var response FnOutput
	if err := JsonPostRequest(client, url, "", &dumpInput, &response); err != nil {
		log.Error().Msgf("dump request failed: %v", err)
	} else if !response.Success {
		log.Error().Msgf("dump request failed: %s", response.Message)
	}
	fmt.Fprintf(os.Stderr, "%sDump invoke done\n", app_name)
}
