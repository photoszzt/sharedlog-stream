package ntypes

import (
	"encoding/json"
	"sharedlog-stream/pkg/common_errors"
	"sharedlog-stream/pkg/commtypes"
)

type NameCityStateIdJSONSerdeG struct {
	commtypes.DefaultJSONSerde
}
type NameCityStateIdMsgpSerdeG struct {
	commtypes.DefaultMsgpSerde
}

var _ = commtypes.SerdeG[NameCityStateId](NameCityStateIdJSONSerdeG{})

func (s NameCityStateIdJSONSerdeG) Encode(value NameCityStateId) ([]byte, error) {
	return json.Marshal(&value)
}

func (s NameCityStateIdJSONSerdeG) Decode(value []byte) (NameCityStateId, error) {
	ncsi := NameCityStateId{}
	if err := json.Unmarshal(value, &ncsi); err != nil {
		return NameCityStateId{}, err
	}
	return ncsi, nil
}

var _ = commtypes.SerdeG[NameCityStateId](NameCityStateIdMsgpSerdeG{})

func (s NameCityStateIdMsgpSerdeG) Encode(value NameCityStateId) ([]byte, error) {
	b := commtypes.PopBuffer()
	buf := *b
	return value.MarshalMsg(buf[:0])
}

func (s NameCityStateIdMsgpSerdeG) Decode(value []byte) (NameCityStateId, error) {
	ncsi := NameCityStateId{}
	if _, err := ncsi.UnmarshalMsg(value); err != nil {
		return NameCityStateId{}, err
	}
	return ncsi, nil
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
