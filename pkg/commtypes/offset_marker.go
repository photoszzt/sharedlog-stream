//go:generate msgp
//msgp:ignore OffsetMarkerJSONSerde OffsetMarkerMsgpSerde
package commtypes

import (
	"encoding/json"
	"sharedlog-stream/pkg/common_errors"
)

type OffsetMarker struct {
	Offset uint64    `json:"offset" msg:"offset"`
	Mark   EpochMark `json:"mark" msg:"mark"`
}

type OffsetMarkerJSONSerde struct{}

var _ Serde[OffsetMarker] = OffsetMarkerJSONSerde{}

func (s OffsetMarkerJSONSerde) Encode(value OffsetMarker) ([]byte, error) {
	return json.Marshal(&value)
}

func (s OffsetMarkerJSONSerde) Decode(value []byte) (OffsetMarker, error) {
	om := OffsetMarker{}
	if err := json.Unmarshal(value, &om); err != nil {
		return OffsetMarker{}, err
	}
	return om, nil
}

type OffsetMarkerMsgpSerde struct{}

var _ Serde[OffsetMarker] = OffsetMarkerMsgpSerde{}

func (s OffsetMarkerMsgpSerde) Encode(value OffsetMarker) ([]byte, error) {
	return value.MarshalMsg(nil)
}

func (s OffsetMarkerMsgpSerde) Decode(value []byte) (OffsetMarker, error) {
	om := OffsetMarker{}
	if _, err := om.UnmarshalMsg(value); err != nil {
		return OffsetMarker{}, err
	}
	return om, nil
}

func GetOffsetMarkerSerde(serdeFormat SerdeFormat) (Serde[OffsetMarker], error) {
	if serdeFormat == JSON {
		return OffsetMarkerJSONSerde{}, nil
	} else if serdeFormat == MSGP {
		return OffsetMarkerMsgpSerde{}, nil
	}
	return nil, common_errors.ErrUnrecognizedSerdeFormat
}
