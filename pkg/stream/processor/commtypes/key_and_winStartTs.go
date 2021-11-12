//go:generate msgp
//msgp:ignore KeyAndWindowStartTsJSONSerde KeyAndWindowStartTsMsgpSerde
package commtypes

import "encoding/json"

type KeyAndWindowStartTs struct {
	Key           []byte `msg:"k" json:"k"`
	WindowStartTs uint64 `msg:"ts" json:"ts"`
}

type KeyAndWindowStartTsJSONSerde struct{}

func (s KeyAndWindowStartTsJSONSerde) Encode(value interface{}) ([]byte, error) {
	val := value.(*KeyAndWindowStartTs)
	return json.Marshal(val)
}

func (s KeyAndWindowStartTsJSONSerde) Decode(value []byte) (interface{}, error) {
	val := KeyAndWindowStartTs{}
	if err := json.Unmarshal(value, &val); err != nil {
		return nil, err
	}
	return val, nil
}

type KeyAndWindowStartTsMsgpSerde struct{}

func (s KeyAndWindowStartTsMsgpSerde) Encode(value interface{}) ([]byte, error) {
	val := value.(*KeyAndWindowStartTs)
	return val.MarshalMsg(nil)
}

func (s KeyAndWindowStartTsMsgpSerde) Decode(value []byte) (interface{}, error) {
	val := KeyAndWindowStartTs{}
	if _, err := val.UnmarshalMsg(value); err != nil {
		return nil, err
	}
	return val, nil
}
