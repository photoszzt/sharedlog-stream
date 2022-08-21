//go:generate msgp
//msgp:ignore NameCityStateIdJSONSerde NameCityStateIdMsgpSerde

package ntypes

import (
	"encoding/json"
	"fmt"
	"sharedlog-stream/pkg/common_errors"
	"sharedlog-stream/pkg/commtypes"
)

type NameCityStateId struct {
	Name  string `msg:"name" json:"name"`
	City  string `msg:"city" json:"city"`
	State string `msg:"state" json:"state"`
	ID    uint64 `msg:"id" json:"id"`
}

func SizeOfNameCityStateId(k NameCityStateId) int64 {
	return 8 + int64(len(k.Name)) + int64(len(k.City)) + int64(len(k.State))
}

var _ = fmt.Stringer(NameCityStateId{})

func (ncsi NameCityStateId) String() string {
	return fmt.Sprintf("NameCityStateId: {Name: %s, City: %s, State: %s, ID: %d}",
		ncsi.Name, ncsi.City, ncsi.State, ncsi.ID)
}

type NameCityStateIdJSONSerde struct{}

var _ = commtypes.Serde(NameCityStateIdJSONSerde{})

func (s NameCityStateIdJSONSerde) Encode(value interface{}) ([]byte, error) {
	ncsi := value.(*NameCityStateId)
	return json.Marshal(ncsi)
}

func (s NameCityStateIdJSONSerde) Decode(value []byte) (interface{}, error) {
	ncsi := NameCityStateId{}
	if err := json.Unmarshal(value, &ncsi); err != nil {
		return nil, err
	}
	return ncsi, nil
}

type NameCityStateIdMsgpSerde struct{}

var _ = commtypes.Serde(NameCityStateIdMsgpSerde{})

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
