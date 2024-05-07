//go:generate msgp
//msgp:ignore OffsetMarkerJSONSerde OffsetMarkerMsgpSerde
package commtypes

import (
	"encoding/json"
	"sharedlog-stream/pkg/common_errors"
)

type OffsetMarker struct {
	ConSeqNums map[string]uint64 `json:"offset" msg:"offset"`
}

type OffsetMarkerJSONSerde struct {
	DefaultJSONSerde
}

var _ = Serde(OffsetMarkerJSONSerde{})

func (s OffsetMarkerJSONSerde) Encode(value interface{}) ([]byte, error) {
	if value == nil {
		return nil, nil
	}
	om := value.(*OffsetMarker)
	return json.Marshal(om)
}

func (s OffsetMarkerJSONSerde) Decode(value []byte) (interface{}, error) {
	om := OffsetMarker{}
	if err := json.Unmarshal(value, &om); err != nil {
		return nil, err
	}
	return om, nil
}

type OffsetMarkerMsgpSerde struct {
	DefaultMsgpSerde
}

var _ = Serde(OffsetMarkerMsgpSerde{})

func (s OffsetMarkerMsgpSerde) Encode(value interface{}) ([]byte, error) {
	if value == nil {
		return nil, nil
	}
	om := value.(*OffsetMarker)
	b := PopBuffer()
	buf := *b
	return om.MarshalMsg(buf[:0])
}

func (s OffsetMarkerMsgpSerde) Decode(value []byte) (interface{}, error) {
	om := OffsetMarker{}
	if _, err := om.UnmarshalMsg(value); err != nil {
		return nil, err
	}
	return om, nil
}

func GetOffsetMarkerSerde(serdeFormat SerdeFormat) (Serde, error) {
	if serdeFormat == JSON {
		return OffsetMarkerJSONSerde{}, nil
	} else if serdeFormat == MSGP {
		return OffsetMarkerMsgpSerde{}, nil
	}
	return nil, common_errors.ErrUnrecognizedSerdeFormat
}
