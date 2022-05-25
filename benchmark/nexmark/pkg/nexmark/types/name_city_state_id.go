//go:generate msgp
//msgp:ignore NameCityStateIdJSONSerde NameCityStateIdMsgpSerde

package types

import "encoding/json"

type NameCityStateId struct {
	Name  string `msg:"name" json:"name"`
	City  string `msg:"city" json:"city"`
	State string `msg:"state" json:"state"`
	ID    uint64 `msg:"id" json:"id"`

	BaseInjTime `msg:"bInjT"`
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
