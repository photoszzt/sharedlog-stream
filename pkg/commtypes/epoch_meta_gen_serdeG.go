package commtypes

import (
	"encoding/json"
	"sharedlog-stream/pkg/common_errors"
)

type EpochMarkerJSONSerdeG struct {
	DefaultJSONSerde
}

var _ = SerdeG[*EpochMarker](EpochMarkerJSONSerdeG{})

func (s EpochMarkerJSONSerdeG) Encode(value *EpochMarker) ([]byte, *[]byte, error) {
	r, err := json.Marshal(value)
	return r, nil, err
}

func (s EpochMarkerJSONSerdeG) Decode(value []byte) (*EpochMarker, error) {
	v := EpochMarker{}
	if err := json.Unmarshal(value, &v); err != nil {
		return nil, err
	}
	return &v, nil
}

type EpochMarkerMsgpSerdeG struct {
	DefaultMsgpSerde
}

var _ = SerdeG[*EpochMarker](EpochMarkerMsgpSerdeG{})

func (s EpochMarkerMsgpSerdeG) Encode(value *EpochMarker) ([]byte, *[]byte, error) {
	b := PopBuffer()
	buf := *b
	r, err := value.MarshalMsg(buf[:0])
	return r, b, err
}

func (s EpochMarkerMsgpSerdeG) Decode(value []byte) (*EpochMarker, error) {
	v := EpochMarker{}
	if _, err := v.UnmarshalMsg(value); err != nil {
		return nil, err
	}
	return &v, nil
}

func GetEpochMarkerSerdeG(serdeFormat SerdeFormat) (SerdeG[*EpochMarker], error) {
	if serdeFormat == JSON {
		return EpochMarkerJSONSerdeG{}, nil
	} else if serdeFormat == MSGP {
		return EpochMarkerMsgpSerdeG{}, nil
	} else {
		return nil, common_errors.ErrUnrecognizedSerdeFormat
	}
}
