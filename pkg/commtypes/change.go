//go:generate msgp
//msgp:ignore Change ChangeJSONSerde ChangeMsgpSerde
package commtypes

import (
	"encoding/json"
	"fmt"
	"sharedlog-stream/pkg/common_errors"
)

type Change struct {
	NewVal interface{}
	OldVal interface{}
}

func (c Change) String() string {
	return fmt.Sprintf("Change: {NewVal: %v, OldVal: %v}", c.NewVal, c.OldVal)
}

type ChangeSerialized struct {
	NewValSerialized []byte `json:"nValSer,omitempty" msg:"nValSer,omitempty"`
	OldValSerialized []byte `json:"oValSer,omitempty" msg:"oValSer,omitempty"`
}

type ChangeJSONSerde struct {
	ValJSONSerde Serde
}

func CastToChangePtr(value interface{}) *Change {
	v, ok := value.(*Change)
	if !ok {
		vtmp := value.(Change)
		v = &vtmp
	}
	return v
}

func (s ChangeJSONSerde) Encode(value interface{}) ([]byte, error) {
	val := CastToChangePtr(value)
	newValEnc, err := s.ValJSONSerde.Encode(val.NewVal)
	if err != nil {
		return nil, err
	}
	oldValEnc, err := s.ValJSONSerde.Encode(val.OldVal)
	if err != nil {
		return nil, err
	}
	c := &ChangeSerialized{
		NewValSerialized: newValEnc,
		OldValSerialized: oldValEnc,
	}
	return json.Marshal(c)
}
func (s ChangeJSONSerde) Decode(value []byte) (interface{}, error) {
	val := ChangeSerialized{}
	if err := json.Unmarshal(value, &val); err != nil {
		return nil, err
	}
	newVal, err := s.ValJSONSerde.Decode(val.NewValSerialized)
	if err != nil {
		return nil, err
	}
	oldVal, err := s.ValJSONSerde.Decode(val.OldValSerialized)
	if err != nil {
		return nil, err
	}
	return Change{
		NewVal: newVal,
		OldVal: oldVal,
	}, nil
}

type ChangeMsgpSerde struct {
	ValMsgpSerde Serde
}

func (s ChangeMsgpSerde) Encode(value interface{}) ([]byte, error) {
	val := CastToChangePtr(value)
	newValEnc, err := s.ValMsgpSerde.Encode(val.NewVal)
	if err != nil {
		return nil, err
	}
	oldValEnc, err := s.ValMsgpSerde.Encode(val.OldVal)
	if err != nil {
		return nil, err
	}
	c := &ChangeSerialized{
		NewValSerialized: newValEnc,
		OldValSerialized: oldValEnc,
	}
	return c.MarshalMsg(nil)
}

func (s ChangeMsgpSerde) Decode(value []byte) (interface{}, error) {
	val := ChangeSerialized{}
	if _, err := val.UnmarshalMsg(value); err != nil {
		return nil, err
	}
	newVal, err := s.ValMsgpSerde.Decode(val.NewValSerialized)
	if err != nil {
		return nil, err
	}
	oldVal, err := s.ValMsgpSerde.Decode(val.OldValSerialized)
	if err != nil {
		return nil, err
	}
	return Change{
		NewVal: newVal,
		OldVal: oldVal,
	}, nil
}

func GetChangeSerde(serdeFormat SerdeFormat, valSerde Serde) (Serde, error) {
	if serdeFormat == JSON {
		return ChangeJSONSerde{
			ValJSONSerde: valSerde,
		}, nil
	} else if serdeFormat == MSGP {
		return ChangeMsgpSerde{
			ValMsgpSerde: valSerde,
		}, nil
	} else {
		return nil, common_errors.ErrUnrecognizedSerdeFormat
	}
}
