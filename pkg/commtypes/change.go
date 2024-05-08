//go:generate msgp
//msgp:ignore Change ChangeJSONSerde ChangeMsgpSerde
package commtypes

import (
	"encoding/json"
	"fmt"
	"sharedlog-stream/pkg/common_errors"
	"sharedlog-stream/pkg/utils"
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

func CastToChangePtr(value interface{}) *Change {
	v, ok := value.(*Change)
	if !ok {
		vtmp := value.(Change)
		v = &vtmp
	}
	return v
}

func convertToChangeSer(value interface{}, valSerde Serde) (*ChangeSerialized, error) {
	if value == nil {
		return nil, nil
	}
	val := CastToChangePtr(value)
	if val == nil {
		return nil, nil
	}
	var newValEnc, oldValEnc []byte
	var err error
	if !utils.IsNil(val.NewVal) {
		newValEnc, err = valSerde.Encode(val.NewVal)
		if err != nil {
			return nil, err
		}
	}
	if !utils.IsNil(val.OldVal) {
		oldValEnc, err = valSerde.Encode(val.OldVal)
		if err != nil {
			return nil, err
		}
	}
	c := &ChangeSerialized{
		NewValSerialized: newValEnc,
		OldValSerialized: oldValEnc,
	}
	return c, nil
}

func changeToChangeSer(value Change, valSerde Serde) (*ChangeSerialized, error) {
	var newValEnc, oldValEnc []byte
	var err error
	if !utils.IsNil(value.NewVal) {
		newValEnc, err = valSerde.Encode(value.NewVal)
		if err != nil {
			return nil, err
		}
	}
	if !utils.IsNil(value.OldVal) {
		oldValEnc, err = valSerde.Encode(value.OldVal)
		if err != nil {
			return nil, err
		}
	}
	c := &ChangeSerialized{
		NewValSerialized: newValEnc,
		OldValSerialized: oldValEnc,
	}
	return c, nil
}

func decodeToChange(val *ChangeSerialized, valSerde Serde) (interface{}, error) {
	return changeSerToChange(val, valSerde)
}

func changeSerToChange(val *ChangeSerialized, valSerde Serde) (Change, error) {
	var newVal, oldVal interface{} = nil, nil
	var err error
	if val.NewValSerialized != nil {
		newVal, err = valSerde.Decode(val.NewValSerialized)
		if err != nil {
			return Change{}, err
		}
	}
	if val.OldValSerialized != nil {
		oldVal, err = valSerde.Decode(val.OldValSerialized)
		if err != nil {
			return Change{}, err
		}
	}
	return Change{
		NewVal: newVal,
		OldVal: oldVal,
	}, nil
}

type ChangeJSONSerde struct {
	DefaultJSONSerde
	ValJSONSerde Serde
}

var _ = Serde(&ChangeJSONSerde{})

func (s *ChangeJSONSerde) Encode(value interface{}) ([]byte, error) {
	c, err := convertToChangeSer(value, s.ValJSONSerde)
	defer func() {
		if s.ValJSONSerde.UsedBufferPool() {
			if c.NewValSerialized != nil {
				PushBuffer(&c.NewValSerialized)
			}
			if c.OldValSerialized != nil {
				PushBuffer(&c.OldValSerialized)
			}
		}
	}()
	if err != nil {
		return nil, err
	}
	return json.Marshal(c)
}

func (s *ChangeJSONSerde) Decode(value []byte) (interface{}, error) {
	val := ChangeSerialized{}
	if err := json.Unmarshal(value, &val); err != nil {
		return nil, err
	}
	return decodeToChange(&val, s.ValJSONSerde)
}

type ChangeMsgpSerde struct {
	DefaultMsgpSerde
	ValMsgpSerde Serde
}

var _ = Serde(&ChangeMsgpSerde{})

func (s *ChangeMsgpSerde) Encode(value interface{}) ([]byte, error) {
	c, err := convertToChangeSer(value, s.ValMsgpSerde)
	defer func() {
		if s.ValMsgpSerde.UsedBufferPool() {
			if c.NewValSerialized != nil {
				PushBuffer(&c.NewValSerialized)
			}
			if c.OldValSerialized != nil {
				PushBuffer(&c.OldValSerialized)
			}
		}
	}()
	if err != nil {
		return nil, err
	}
	b := PopBuffer()
	buf := *b
	return c.MarshalMsg(buf[:0])
}

func (s *ChangeMsgpSerde) Decode(value []byte) (interface{}, error) {
	val := ChangeSerialized{}
	if _, err := val.UnmarshalMsg(value); err != nil {
		return nil, err
	}
	return decodeToChange(&val, s.ValMsgpSerde)
}

func GetChangeSerde(serdeFormat SerdeFormat, valSerde Serde) (Serde, error) {
	if serdeFormat == JSON {
		return &ChangeJSONSerde{
			ValJSONSerde: valSerde,
		}, nil
	} else if serdeFormat == MSGP {
		return &ChangeMsgpSerde{
			ValMsgpSerde: valSerde,
		}, nil
	} else {
		return nil, common_errors.ErrUnrecognizedSerdeFormat
	}
}
