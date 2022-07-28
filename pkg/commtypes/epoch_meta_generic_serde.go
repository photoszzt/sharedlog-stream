package commtypes

import (
	"encoding/json"
	"sharedlog-stream/pkg/common_errors"
)

type EpochMarkerJSONSerdeG struct{}

var _ = SerdeG[EpochMarker](EpochMarkerJSONSerdeG{})

func (s EpochMarkerJSONSerdeG) Encode(value EpochMarker) ([]byte, error) {
	return json.Marshal(&value)
}

func (s EpochMarkerJSONSerdeG) Decode(value []byte) (EpochMarker, error) {
	em := EpochMarker{}
	if err := json.Unmarshal(value, &em); err != nil {
		return em, err
	}
	return em, nil
}

type EpochMarkerMsgpSerdeG struct{}

var _ = SerdeG[EpochMarker](EpochMarkerMsgpSerdeG{})

func (s EpochMarkerMsgpSerdeG) Encode(value EpochMarker) ([]byte, error) {
	return value.MarshalMsg(nil)
}
func (s EpochMarkerMsgpSerdeG) Decode(value []byte) (EpochMarker, error) {
	em := EpochMarker{}
	if _, err := em.UnmarshalMsg(value); err != nil {
		return EpochMarker{}, err
	}
	return em, nil
}

func GetEpochMarkerSerdeG(serdeFormat SerdeFormat) (SerdeG[EpochMarker], error) {
	if serdeFormat == JSON {
		return EpochMarkerJSONSerdeG{}, nil
	} else if serdeFormat == MSGP {
		return EpochMarkerMsgpSerdeG{}, nil
	} else {
		return nil, common_errors.ErrUnrecognizedSerdeFormat
	}
}
