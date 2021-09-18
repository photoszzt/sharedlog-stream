package spike_detection

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/benchmark/nexmark/pkg/nexmark/utils"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/stream/processor"
	"strconv"
	"time"

	"cs.utexas.edu/zjia/faas/types"
)

type rawSensorData struct {
	ts       time.Time
	epoc     uint32
	devId    string
	temp     float64
	humidity float64
	light    float64
	voltage  float64
}

type spikeDetectionSource struct {
	env            types.Environment
	sensorDataList []*rawSensorData
}

func NewSpikeDetectionSource(env types.Environment) types.FuncHandler {
	return &spikeDetectionSource{
		env:            env,
		sensorDataList: make([]*rawSensorData, 0),
	}
}

func (h *spikeDetectionSource) Call(ctx context.Context, input []byte) ([]byte, error) {
	sp := &common.SourceParam{}
	err := json.Unmarshal(input, sp)
	if err != nil {
		return nil, err
	}
	err = h.parseFile(sp.FileName)
	if err != nil {
		return nil, err
	}
	output := h.eventGeneration(ctx, h.env, sp)
	encodedOutput, err := json.Marshal(output)
	if err != nil {
		panic(err)
	}
	return utils.CompressData(encodedOutput), nil
}

func (h *spikeDetectionSource) eventGeneration(ctx context.Context,
	env types.Environment, sp *common.SourceParam) *common.FnOutput {
	err := h.parseFile(sp.FileName)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: fmt.Sprintf("parse input file failed: %v", err),
		}
	}

	stream, err := sharedlog_stream.NewSharedLogStream(ctx, env, sp.TopicName)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: fmt.Sprintf("NewSharedlogStream failed: %v", err),
		}
	}
	var sdSerde processor.Serde
	if sp.SerdeFormat == uint8(common.JSON) {
		sdSerde = SensorDataJSONSerde{}
	} else if sp.SerdeFormat == uint8(common.MSGP) {
		sdSerde = SensorDataMsgpSerde{}
	} else {
		return &common.FnOutput{
			Success: false,
			Message: fmt.Sprintf("serde format should be either json or msgp; but %v is given", sp.SerdeFormat),
		}
	}
	msgSerde, err := common.GetMsgSerde(sp.SerdeFormat)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: err.Error(),
		}
	}
	strSerde := processor.StringSerde{}
	latencies := make([]int, 0, 128)
	duration := time.Duration(sp.Duration) * time.Second
	startTime := time.Now()
	idx := 0
	for {
		if duration != 0 && time.Since(startTime) >= duration || duration == 0 {
			break
		}
		event := h.sensorDataList[idx]
		sd := SensorData{
			Val:       event.temp,
			Timestamp: uint64(event.ts.Unix()),
		}
		encoded, err := sdSerde.Encode(sd)
		if err != nil {
			return &common.FnOutput{
				Success: false,
				Message: fmt.Sprintf("event serialization failed: %v", err),
			}
		}

		key_encoded, err := strSerde.Encode(event.devId)
		if err != nil {
			return &common.FnOutput{
				Success: false,
				Message: fmt.Sprintf("key serialization failed: %v", err),
			}
		}
		msgEncoded, err := msgSerde.Encode(key_encoded, encoded)
		if err != nil {
			return &common.FnOutput{
				Success: false,
				Message: fmt.Sprintf("msg serialization failed: %v", err),
			}
		}
		pushStart := time.Now()
		_, err = stream.Push(msgEncoded)
		if err != nil {
			return &common.FnOutput{
				Success: false,
				Message: fmt.Sprintf("stream push failed: %v", err),
			}
		}
		elapsed := time.Since(pushStart)
		latencies = append(latencies, int(elapsed.Microseconds()))
	}
	return &common.FnOutput{
		Success:   true,
		Duration:  time.Since(startTime).Seconds(),
		Latencies: latencies,
	}
}

func (h *spikeDetectionSource) parseFile(fileName string) error {
	csvFile, err := os.Open(fileName)
	if err != nil {
		return err
	}
	defer csvFile.Close()

	reader := csv.NewReader(csvFile)
	reader.Comma = ' '
	csvData, err := reader.ReadAll()
	if err != nil {
		return err
	}
	for _, each := range csvData {
		epoc, err := strconv.ParseUint(each[2], 10, 32)
		if err != nil {
			return err
		}
		temp, err := strconv.ParseFloat(each[4], 64)
		if err != nil {
			return err
		}
		humidity, err := strconv.ParseFloat(each[5], 64)
		if err != nil {
			return err
		}
		light, err := strconv.ParseFloat(each[6], 64)
		if err != nil {
			return err
		}
		voltage, err := strconv.ParseFloat(each[7], 64)
		if err != nil {
			return err
		}
		date := each[0][5:]
		timeStr := each[1][5:]
		var year, month, day int
		_, err = fmt.Sscanf(date, "%d-%d-%d", &year, &month, &day)
		if err != nil {
			return err
		}
		var hour, min int
		var sec float64
		_, err = fmt.Sscanf(timeStr, "%d:%d:%f", &hour, &min, &sec)
		if err != nil {
			return err
		}
		subsec := sec - math.Floor(sec)
		ts := time.Date(year, time.Month(month), day, hour, min, int(math.Floor(sec)), int(subsec*1e9),
			time.UTC)
		sd := &rawSensorData{
			ts:       ts,
			epoc:     uint32(epoc),
			devId:    each[3],
			temp:     temp,
			humidity: humidity,
			light:    light,
			voltage:  voltage,
		}
		h.sensorDataList = append(h.sensorDataList, sd)
	}
	return nil
}
