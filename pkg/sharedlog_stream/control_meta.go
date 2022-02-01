//go:generate msgp
//msgp:ignore ControlMetadataJSONSerde ControlMetadataMsgpSerde
package sharedlog_stream

import "encoding/json"

type ControlMetadata struct {
	Stages     map[string]uint8 `json:"sg,omitempty" msgp:"sg,omitempty"`
	Key        []byte           `json:"k,omitempty" msgp:"k,omitempty"`
	Epoch      uint32           `json:"ep" msgp:"ep"`
	InstanceId uint8            `json:"iid,omitempty" msgp:"iid,omitempty"`
}

type ControlMetadataJSONSerde struct{}

func (s ControlMetadataJSONSerde) Encode(value interface{}) ([]byte, error) {
	rf := value.(*ControlMetadata)
	return json.Marshal(rf)
}

func (s ControlMetadataJSONSerde) Decode(value []byte) (interface{}, error) {
	rf := ControlMetadata{}
	if err := json.Unmarshal(value, &rf); err != nil {
		return nil, err
	}
	return rf, nil
}

type ControlMetadataMsgpSerde struct{}

func (s ControlMetadataMsgpSerde) Encode(value interface{}) ([]byte, error) {
	rf := value.(*ControlMetadata)
	return rf.MarshalMsg(nil)
}

func (s ControlMetadataMsgpSerde) Decode(value []byte) (interface{}, error) {
	rf := ControlMetadata{}
	if _, err := rf.UnmarshalMsg(value); err != nil {
		return nil, err
	}
	return rf, nil
}
