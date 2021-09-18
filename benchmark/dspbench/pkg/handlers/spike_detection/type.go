//go:generate greenpack
//msgp:ignore SensorDataMsgpEncoder
package spike_detection

import (
	"encoding/json"
	"sharedlog-stream/pkg/stream/processor"
)

type SourceParam struct {
	TopicName   string `json:"topicName"`
	FileName    string `json:"fname"`
	Duration    uint32 `json:"duration"` // in sec
	SerdeFormat uint8  `json:"serdeFormat"`
}

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
