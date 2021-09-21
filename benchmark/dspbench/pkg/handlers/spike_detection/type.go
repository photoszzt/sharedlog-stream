//go:generate greenpack
//msgp:ignore SensorDataMsgpEncoder
package spike_detection

import (
	"encoding/json"
	"sharedlog-stream/pkg/stream/processor"
)

type SensorData struct {
	Val       float64 `json:"val" zid:"0" msg:"val"`
	Timestamp uint64  `json:"ts" zid:"1" msg:"ts"`
}

type SensorDataMsgpSerde struct{}

var _ = processor.Serde(SensorDataMsgpSerde{})

type SensorDataJSONSerde struct{}

var _ = processor.Serde(SensorDataJSONSerde{})

func (e SensorDataMsgpSerde) Encode(value interface{}) ([]byte, error) {
	sd := value.(*SensorData)
	return sd.MarshalMsg(nil)
}

func (e SensorDataJSONSerde) Encode(value interface{}) ([]byte, error) {
	sd := value.(*SensorData)
	return json.Marshal(sd)
}

func (d SensorDataMsgpSerde) Decode(value []byte) (interface{}, error) {
	sd := SensorData{}
	_, err := sd.UnmarshalMsg(value)
	if err != nil {
		return nil, err
	}
	return sd, nil
}

func (d SensorDataJSONSerde) Decode(value []byte) (interface{}, error) {
	sd := SensorData{}
	if err := json.Unmarshal(value, &sd); err != nil {
		return nil, err
	}
	return sd, nil
}

type ValAndAvg struct {
	Val float64 `json:"val" zid:"0" msg:"val"`
	Avg float64 `json:"avg" zid:"1" msg:"avg"`
}

type SumAndHist struct {
	Val     float64   `json:"val" zid:"0" msg:"val"`
	Sum     float64   `json:"sum" zid:"1" msg:"sum"`
	history []float64 `json:"hist" zid:"2" msg:"hist"`
}

type SumAndHistJSONSerde struct{}

func (s SumAndHistJSONSerde) Encode(value interface{}) ([]byte, error) {
	val := value.(*SumAndHist)
	return json.Marshal(val)
}

func (s SumAndHistJSONSerde) Decode(value []byte) (interface{}, error) {
	sh := SumAndHist{}
	if err := json.Unmarshal(value, &sh); err != nil {
		return nil, err
	}
	return sh, nil
}

type SumAndHistMsgpSerde struct{}

func (s SumAndHistMsgpSerde) Encode(value interface{}) ([]byte, error) {
	val := value.(*SumAndHist)
	return val.MarshalMsg(nil)
}

func (s SumAndHistMsgpSerde) Decode(value []byte) (interface{}, error) {
	sh := SumAndHist{}
	_, err := sh.UnmarshalMsg(value)
	if err != nil {
		return nil, err
	}
	return value, nil
}
