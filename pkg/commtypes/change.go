//go:generate msgp
//msgp:ignore Change ChangeJSONSerde ChangeMsgpSerde
package commtypes

import (
	"encoding/json"
	"fmt"
	"sharedlog-stream/pkg/common_errors"

	"4d63.com/optional"
)

type Change[T any] struct {
	NewVal optional.Optional[T]
	OldVal optional.Optional[T]
}

func (c Change[T]) String() string {
	return fmt.Sprintf("Change: {NewVal: %v, OldVal: %v}", c.NewVal, c.OldVal)
}

type ChangeSerialized struct {
	NewValSerialized []byte `json:"nValSer,omitempty" msg:"nValSer,omitempty"`
	OldValSerialized []byte `json:"oValSer,omitempty" msg:"oValSer,omitempty"`
}

type ChangeJSONSerde[valT any] struct {
	ValJSONSerde Serde[valT]
}

var _ Serde[Change[int]] = ChangeJSONSerde[int]{}

func CastToChangePtr[T any](value interface{}) *Change[T] {
	v, ok := value.(*Change[T])
	if !ok {
		vtmp := value.(Change[T])
		v = &vtmp
	}
	return v
}

func convertToChangeSer[valT any](value Change[valT], valSerde Serde[valT]) (*ChangeSerialized, error) {
	var newValEnc, oldValEnc []byte
	var err error
	if newVal, ok := value.NewVal.Get(); ok {
		newValEnc, err = valSerde.Encode(newVal)
		if err != nil {
			return nil, err
		}
	}
	if oldVal, ok := value.OldVal.Get(); ok {
		oldValEnc, err = valSerde.Encode(oldVal)
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

func decodeToChange[valT any](val *ChangeSerialized, valSerde Serde[valT]) (Change[valT], error) {
	newVal, oldVal := optional.Empty[valT](), optional.Empty[valT]()
	if val.NewValSerialized != nil {
		newValTmp, err := valSerde.Decode(val.NewValSerialized)
		if err != nil {
			return Change[valT]{}, err
		}
		newVal = optional.Of(newValTmp)
	}
	if val.OldValSerialized != nil {
		oldValTmp, err := valSerde.Decode(val.OldValSerialized)
		if err != nil {
			return Change[valT]{}, err
		}
		oldVal = optional.Of(oldValTmp)
	}
	return Change[valT]{
		NewVal: newVal,
		OldVal: oldVal,
	}, nil
}

func (s ChangeJSONSerde[valT]) Encode(value Change[valT]) ([]byte, error) {
	c, err := convertToChangeSer(value, s.ValJSONSerde)
	if err != nil {
		return nil, err
	}
	return json.Marshal(c)
}
func (s ChangeJSONSerde[valT]) Decode(value []byte) (Change[valT], error) {
	val := ChangeSerialized{}
	if err := json.Unmarshal(value, &val); err != nil {
		return Change[valT]{}, err
	}
	return decodeToChange(&val, s.ValJSONSerde)
}

type ChangeMsgpSerde[valT any] struct {
	ValMsgpSerde Serde[valT]
}

var _ = Serde[Change[int]](ChangeMsgpSerde[int]{})

func (s ChangeMsgpSerde[valT]) Encode(value Change[valT]) ([]byte, error) {
	c, err := convertToChangeSer(value, s.ValMsgpSerde)
	if err != nil {
		return nil, err
	}
	return c.MarshalMsg(nil)
}

func (s ChangeMsgpSerde[valT]) Decode(value []byte) (Change[valT], error) {
	val := ChangeSerialized{}
	if _, err := val.UnmarshalMsg(value); err != nil {
		return Change[valT]{}, err
	}
	return decodeToChange(&val, s.ValMsgpSerde)
}

func GetChangeSerde[valT any](serdeFormat SerdeFormat, valSerde Serde[valT]) (Serde[Change[valT]], error) {
	if serdeFormat == JSON {
		return ChangeJSONSerde[valT]{
			ValJSONSerde: valSerde,
		}, nil
	} else if serdeFormat == MSGP {
		return ChangeMsgpSerde[valT]{
			ValMsgpSerde: valSerde,
		}, nil
	} else {
		return nil, common_errors.ErrUnrecognizedSerdeFormat
	}
}
