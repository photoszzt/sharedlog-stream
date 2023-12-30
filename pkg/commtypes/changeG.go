package commtypes

import (
	"encoding/json"
	"sharedlog-stream/pkg/common_errors"
	"sharedlog-stream/pkg/optional"
)

type ChangeG[V any] struct {
	NewVal optional.Option[V]
	OldVal optional.Option[V]
}

func NewChangeOnlyNewValG[V any](newVal V) ChangeG[V] {
	return ChangeG[V]{
		NewVal: optional.Some(newVal),
		OldVal: optional.None[V](),
	}
}

func NewChangeOnlyOldValG[V any](oldVal V) ChangeG[V] {
	return ChangeG[V]{
		NewVal: optional.None[V](),
		OldVal: optional.Some(oldVal),
	}
}

func NewChangeG[V any](newVal, oldVal V) ChangeG[V] {
	return ChangeG[V]{
		NewVal: optional.Some(newVal),
		OldVal: optional.Some(oldVal),
	}
}

type ChangeGJSONSerdeG[V any] struct {
	ValJSONSerde SerdeG[V]
}

func changeGToChangeSer[V any](value ChangeG[V], valSerde SerdeG[V]) (*ChangeSerialized, error) {
	var newValEnc, oldValEnc []byte
	var err error
	newValMsg, ok := value.NewVal.Take()
	if ok {
		newValEnc, err = valSerde.Encode(newValMsg)
		if err != nil {
			return nil, err
		}
	}
	oldValMsg, ok := value.OldVal.Take()
	if ok {
		oldValEnc, err = valSerde.Encode(oldValMsg)
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

func changeSerToChangeG[V any](val *ChangeSerialized, valSerde SerdeG[V]) (ChangeG[V], error) {
	newValOp := optional.None[V]()
	oldValOp := optional.None[V]()
	if val.NewValSerialized != nil {
		newVal, err := valSerde.Decode(val.NewValSerialized)
		if err != nil {
			return ChangeG[V]{}, err
		}
		newValOp = optional.Some(newVal)
	}
	if val.OldValSerialized != nil {
		oldVal, err := valSerde.Decode(val.OldValSerialized)
		if err != nil {
			return ChangeG[V]{}, err
		}
		oldValOp = optional.Some(oldVal)
	}
	return ChangeG[V]{
		NewVal: newValOp,
		OldVal: oldValOp,
	}, nil
}

var _ = SerdeG[ChangeG[int]](ChangeGJSONSerdeG[int]{})

func (s ChangeGJSONSerdeG[V]) Encode(value ChangeG[V]) ([]byte, error) {
	c, err := changeGToChangeSer(value, s.ValJSONSerde)
	if err != nil {
		return nil, err
	}
	return json.Marshal(c)
}

func (s ChangeGJSONSerdeG[V]) Decode(value []byte) (ChangeG[V], error) {
	val := ChangeSerialized{}
	if err := json.Unmarshal(value, &val); err != nil {
		return ChangeG[V]{}, err
	}
	return changeSerToChangeG(&val, s.ValJSONSerde)
}

type ChangeGMsgpSerdeG[V any] struct {
	ValMsgpSerde SerdeG[V]
}

var _ = SerdeG[ChangeG[int]](ChangeGMsgpSerdeG[int]{})

func (s ChangeGMsgpSerdeG[V]) Encode(value ChangeG[V]) ([]byte, error) {
	c, err := changeGToChangeSer(value, s.ValMsgpSerde)
	if err != nil {
		return nil, err
	}
	return c.MarshalMsg(nil)
}

func (s ChangeGMsgpSerdeG[V]) Decode(value []byte) (ChangeG[V], error) {
	val := ChangeSerialized{}
	if _, err := val.UnmarshalMsg(value); err != nil {
		return ChangeG[V]{}, err
	}
	return changeSerToChangeG(&val, s.ValMsgpSerde)
}

func GetChangeGSerdeG[V any](serdeFormat SerdeFormat, valSerde SerdeG[V]) (SerdeG[ChangeG[V]], error) {
	if serdeFormat == JSON {
		return ChangeGJSONSerdeG[V]{
			ValJSONSerde: valSerde,
		}, nil
	} else if serdeFormat == MSGP {
		return ChangeGMsgpSerdeG[V]{
			ValMsgpSerde: valSerde,
		}, nil
	} else {
		return nil, common_errors.ErrUnrecognizedSerdeFormat
	}
}
