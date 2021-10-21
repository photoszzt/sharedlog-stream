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
	output := h.eventGeneration(ctx, sp)
	encodedOutput, err := json.Marshal(output)
	if err != nil {
		return nil, err
	}
	return utils.CompressData(encodedOutput), nil
}

func encode_sensor_event(keySerde processor.Serde,
	valSerde processor.Serde,
	msgSerde processor.MsgSerde, key string, val *SensorData) ([]byte, error) {

	val_encoded, err := valSerde.Encode(val)
	if err != nil {
		return nil, fmt.Errorf("event serialization failed: %v", err)
	}

	key_encoded, err := keySerde.Encode(key)
	if err != nil {
		return nil, fmt.Errorf("key serialization failed: %v", err)
	}
	/*
		fmt.Fprintf(os.Stderr, "key is %v, val is %v\n", string(key_encoded),
			string(val_encoded))
	*/
	msgEncoded, err := msgSerde.Encode(key_encoded, val_encoded)
	if err != nil {
		return nil, fmt.Errorf("msg serialization failed: %v", err)
	}
	/*
		fmt.Fprintf(os.Stderr, "generated msg is %v\n", string(msgEncoded))
	*/
	return msgEncoded, nil
}

func (h *spikeDetectionSource) eventGeneration(ctx context.Context, sp *common.SourceParam) *common.FnOutput {
	stream, err := sharedlog_stream.NewSharedLogStream(ctx, h.env, sp.TopicName)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: fmt.Sprintf("NewSharedlogStream failed: %v", err),
		}
	}
	var sdSerde processor.Serde
	if sp.SerdeFormat == uint8(processor.JSON) {
		sdSerde = SensorDataJSONSerde{}
	} else if sp.SerdeFormat == uint8(processor.MSGP) {
		sdSerde = SensorDataMsgpSerde{}
	} else {
		return &common.FnOutput{
			Success: false,
			Message: fmt.Sprintf("serde format should be either json or msgp; but %v is given", sp.SerdeFormat),
		}
	}
	msgSerde, err := processor.GetMsgSerde(sp.SerdeFormat)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: err.Error(),
		}
	}
	strSerde := processor.StringSerde{}
	latencies := make([]int, 0, 128)
	numEvents := sp.NumEvents
	duration := time.Duration(sp.Duration) * time.Second
	startTime := time.Now()
	idx := 0
	for {
		if duration != 0 && time.Since(startTime) >= duration {
			break
		}
		if numEvents != 0 && idx == int(numEvents) {
			break
		}

		event := h.sensorDataList[idx]
		sd := SensorData{
			Val:       event.temp,
			Timestamp: uint64(event.ts.Unix()),
		}
		msgEncoded, err := encode_sensor_event(strSerde, sdSerde, msgSerde, event.devId, &sd)
		if err != nil {
			return &common.FnOutput{
				Success: false,
				Message: err.Error(),
			}
		}
		pushStart := time.Now()
		_, err = stream.Push(msgEncoded, 0)
		if err != nil {
			return &common.FnOutput{
				Success: false,
				Message: fmt.Sprintf("stream push failed: %v", err),
			}
		}
		elapsed := time.Since(pushStart)
		latencies = append(latencies, int(elapsed.Microseconds()))
		idx += 1
		if idx >= len(h.sensorDataList) {
			// generate more events
			// idx = 0

			// generate events until end
			break
		}
	}
	return &common.FnOutput{
		Success:   true,
		Duration:  time.Since(startTime).Seconds(),
		Latencies: map[string][]int{"e2e": latencies},
	}
}

func (h *spikeDetectionSource) parseFile(fileName string) error {
	csvFile, err := os.Open(fileName)
	if err != nil {
		return fmt.Errorf("fail to open file %v: %v", fileName, err)
	}
	defer csvFile.Close()

	reader := csv.NewReader(csvFile)
	reader.Comma = ' '
	csvData, err := reader.ReadAll()
	if err != nil {
		return err
	}
	for _, each := range csvData {
		if each[2] == "" {
			continue
		}
		epoc, err := strconv.ParseUint(each[2], 10, 32)
		if err != nil {
			return err
		}

		if each[4] == "" {
			continue
		}
		temp, err := strconv.ParseFloat(each[4], 64)
		if err != nil {
			return err
		}

		if each[5] == "" {
			continue
		}
		humidity, err := strconv.ParseFloat(each[5], 64)
		if err != nil {
			return err
		}

		if each[6] == "" {
			continue
		}
		light, err := strconv.ParseFloat(each[6], 64)
		if err != nil {
			return err
		}

		if each[6] == "" {
			continue
		}

		voltage, err := strconv.ParseFloat(each[7], 64)
		if err != nil {
			return err
		}

		date := each[0]
		timeStr := each[1]

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
