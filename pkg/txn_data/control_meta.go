//go:generate msgp
//msgp:ignore ControlMetadataJSONSerde ControlMetadataMsgpSerde
package txn_data

import (
	"encoding/json"
	"sharedlog-stream/pkg/commtypes"
)

type ControlMetadata struct {
	// number of instances for each stage
	Config map[string]uint8 `json:"sg,omitempty" msgp:"sg,omitempty"`

	// topic of the stream
	Topic string `json:"tp,omitempty" msgp:"tp,omitempty"`
	// old scale epoch done
	FinishedPrevTask string `json:"fpt,omitempty" msgp:"fpt,omitempty"`

	// key of msg
	Key []byte `json:"k,omitempty" msgp:"k,omitempty"`

	Epoch uint64 `json:"ep,omitempty" msgp:"ep,omitempty"`
	// old instance id
	InstanceId uint8 `json:"iid,omitempty" msgp:"iid,omitempty"`

	// substream id that the key stores to
	SubstreamId uint8 `json:"sid,omitempty" msgp:"sid,omitempty"`
}

type ControlMetadataJSONSerde struct{}

var _ commtypes.Serde[ControlMetadata] = ControlMetadataJSONSerde{}

func (s ControlMetadataJSONSerde) Encode(value ControlMetadata) ([]byte, error) {
	return json.Marshal(&value)
}

func (s ControlMetadataJSONSerde) Decode(value []byte) (ControlMetadata, error) {
	rf := ControlMetadata{}
	if err := json.Unmarshal(value, &rf); err != nil {
		return ControlMetadata{}, err
	}
	return rf, nil
}

type ControlMetadataMsgpSerde struct{}

var _ = commtypes.Serde[ControlMetadata](ControlMetadataMsgpSerde{})

func (s ControlMetadataMsgpSerde) Encode(value ControlMetadata) ([]byte, error) {
	return value.MarshalMsg(nil)
}

func (s ControlMetadataMsgpSerde) Decode(value []byte) (ControlMetadata, error) {
	rf := ControlMetadata{}
	if _, err := rf.UnmarshalMsg(value); err != nil {
		return ControlMetadata{}, err
	}
	return rf, nil
}
