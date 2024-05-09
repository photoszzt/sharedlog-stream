package ntypes

import (
	"encoding/json"
	"sharedlog-stream/pkg/common_errors"
	"sharedlog-stream/pkg/commtypes"
)

type NameCityStateIdJSONSerdeG struct {
	commtypes.DefaultJSONSerde
}

var _ = commtypes.SerdeG[NameCityStateId](NameCityStateIdJSONSerdeG{})

func (s NameCityStateIdJSONSerdeG) Encode(value NameCityStateId) ([]byte, *[]byte, error) {
	r, err := json.Marshal(value)
	return r, nil, err
}

func (s NameCityStateIdJSONSerdeG) Decode(value []byte) (NameCityStateId, error) {
	v := NameCityStateId{}
	if err := json.Unmarshal(value, &v); err != nil {
		return NameCityStateId{}, err
	}
	return v, nil
}

type NameCityStateIdMsgpSerdeG struct {
	commtypes.DefaultMsgpSerde
}

var _ = commtypes.SerdeG[NameCityStateId](NameCityStateIdMsgpSerdeG{})

func (s NameCityStateIdMsgpSerdeG) Encode(value NameCityStateId) ([]byte, *[]byte, error) {
	b := commtypes.PopBuffer()
	buf := *b
	r, err := value.MarshalMsg(buf[:0])
	return r, b, err
}

func (s NameCityStateIdMsgpSerdeG) Decode(value []byte) (NameCityStateId, error) {
	v := NameCityStateId{}
	if _, err := v.UnmarshalMsg(value); err != nil {
		return NameCityStateId{}, err
	}
	return v, nil
}

func GetNameCityStateIdSerdeG(serdeFormat commtypes.SerdeFormat) (commtypes.SerdeG[NameCityStateId], error) {
	if serdeFormat == commtypes.JSON {
		return NameCityStateIdJSONSerdeG{}, nil
	} else if serdeFormat == commtypes.MSGP {
		return NameCityStateIdMsgpSerdeG{}, nil
	} else {
		return nil, common_errors.ErrUnrecognizedSerdeFormat
	}
}
