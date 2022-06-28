//go:generate msgp
//msgp:ignore NameCityStateIdJSONSerde NameCityStateIdMsgpSerde

package types

import (
	"encoding/json"
	"sharedlog-stream/pkg/common_errors"
	"sharedlog-stream/pkg/commtypes"
)

type NameCityStateId struct {
	Name  string `msg:"name" json:"name"`
	City  string `msg:"city" json:"city"`
	State string `msg:"state" json:"state"`
	ID    uint64 `msg:"id" json:"id"`
}

type NameCityStateIdJSONSerde struct{}

func (s NameCityStateIdJSONSerde) Encode(value interface{}) ([]byte, error) {
	ncsi := value.(*NameCityStateId)
	return json.Marshal(ncsi)
}

func (s NameCityStateIdJSONSerde) Decode(value []byte) (interface{}, error) {
	ncsi := &NameCityStateId{}
	if err := json.Unmarshal(value, ncsi); err != nil {
		return nil, err
	}
	return ncsi, nil
}

type NameCityStateIdMsgpSerde struct{}

func (s NameCityStateIdMsgpSerde) Encode(value interface{}) ([]byte, error) {
	ncsi := value.(*NameCityStateId)
	return ncsi.MarshalMsg(nil)
}

func (s NameCityStateIdMsgpSerde) Decode(value []byte) (interface{}, error) {
	ncsi := NameCityStateId{}
	if _, err := ncsi.UnmarshalMsg(value); err != nil {
		return nil, err
	}
	return ncsi, nil
}

func GetNameCityStateIdSerde(serdeFormat commtypes.SerdeFormat) (commtypes.Serde, error) {
	var ncsiSerde commtypes.Serde
	if serdeFormat == commtypes.JSON {
		ncsiSerde = NameCityStateIdJSONSerde{}
	} else if serdeFormat == commtypes.MSGP {
		ncsiSerde = NameCityStateIdMsgpSerde{}
	} else {
		return nil, common_errors.ErrUnrecognizedSerdeFormat
	}
	return ncsiSerde, nil
}
