package commtypes

import (
	"encoding/json"
	"sharedlog-stream/pkg/common_errors"
)

type ChangeSizeG[V any] struct {
	ValSizeFunc func(v V) int64
}

func (cs ChangeSizeG[V]) SizeOfChange(c Change) int64 {
	return cs.ValSizeFunc(c.NewVal.(V)) + cs.ValSizeFunc(c.OldVal.(V))
}

type ChangeJSONSerdeG struct {
	ValJSONSerde Serde
}

var _ = SerdeG[Change](&ChangeJSONSerdeG{})

func (s ChangeJSONSerdeG) Encode(value Change) ([]byte, error) {
	c, err := changeToChangeSer(value, s.ValJSONSerde)
	if err != nil {
		return nil, err
	}
	return json.Marshal(c)
}

func (s ChangeJSONSerdeG) Decode(value []byte) (Change, error) {
	val := ChangeSerialized{}
	if err := json.Unmarshal(value, &val); err != nil {
		return Change{}, err
	}
	return changeSerToChange(&val, s.ValJSONSerde)
}

type ChangeMsgpSerdeG struct {
	ValMsgpSerde Serde
}

var _ = SerdeG[Change](ChangeMsgpSerdeG{})

func (s ChangeMsgpSerdeG) Encode(value Change) ([]byte, error) {
	c, err := changeToChangeSer(value, s.ValMsgpSerde)
	if err != nil {
		return nil, err
	}
	return c.MarshalMsg(nil)
}

func (s ChangeMsgpSerdeG) Decode(value []byte) (Change, error) {
	val := ChangeSerialized{}
	if _, err := val.UnmarshalMsg(value); err != nil {
		return Change{}, err
	}
	return changeSerToChange(&val, s.ValMsgpSerde)
}

func GetChangeSerdeG(serdeFormat SerdeFormat, valSerde Serde) (SerdeG[Change], error) {
	if serdeFormat == JSON {
		return ChangeJSONSerdeG{
			ValJSONSerde: valSerde,
		}, nil
	} else if serdeFormat == MSGP {
		return ChangeMsgpSerdeG{
			ValMsgpSerde: valSerde,
		}, nil
	} else {
		return nil, common_errors.ErrUnrecognizedSerdeFormat
	}
}
