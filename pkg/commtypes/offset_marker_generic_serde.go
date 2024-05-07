package commtypes

import (
	"encoding/json"
	"sharedlog-stream/pkg/common_errors"
)

type (
	OffsetMarkerJSONSerdeG struct {
		DefaultJSONSerde
	}
	OffsetMarkerMsgpSerdeG struct {
		DefaultMsgpSerde
	}
)

var _ = SerdeG[OffsetMarker](OffsetMarkerJSONSerdeG{})

func (s OffsetMarkerJSONSerdeG) Encode(value OffsetMarker) ([]byte, error) {
	return json.Marshal(&value)
}

func (s OffsetMarkerJSONSerdeG) Decode(value []byte) (OffsetMarker, error) {
	om := OffsetMarker{}
	if err := json.Unmarshal(value, &om); err != nil {
		return OffsetMarker{}, err
	}
	return om, nil
}

var _ = SerdeG[OffsetMarker](OffsetMarkerMsgpSerdeG{})

func (s OffsetMarkerMsgpSerdeG) Encode(value OffsetMarker) ([]byte, error) {
	b := PopBuffer()
	buf := *b
	return value.MarshalMsg(buf[:0])
}

func (s OffsetMarkerMsgpSerdeG) Decode(value []byte) (OffsetMarker, error) {
	om := OffsetMarker{}
	if _, err := om.UnmarshalMsg(value); err != nil {
		return OffsetMarker{}, err
	}
	return om, nil
}

func GetOffsetMarkerSerdeG(serdeFormat SerdeFormat) (SerdeG[OffsetMarker], error) {
	if serdeFormat == JSON {
		return OffsetMarkerJSONSerdeG{}, nil
	} else if serdeFormat == MSGP {
		return OffsetMarkerMsgpSerdeG{}, nil
	}
	return nil, common_errors.ErrUnrecognizedSerdeFormat
}
