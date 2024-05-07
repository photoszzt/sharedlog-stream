package commtypes

import (
	"encoding/json"
	"sharedlog-stream/pkg/common_errors"
	"sharedlog-stream/pkg/utils"
)

type ChangeSize[V any] struct {
	ValSizeFunc func(v V) int64
}

func (cs ChangeSize[V]) SizeOfChange(c Change) int64 {
	total := int64(0)
	if !utils.IsNil(c.NewVal) {
		total += cs.ValSizeFunc(c.NewVal.(V))
	}
	if !utils.IsNil(c.OldVal) {
		total += cs.ValSizeFunc(c.OldVal.(V))
	}
	return total
}

type ChangeJSONSerdeG struct {
	DefaultJSONSerde
	ValJSONSerde Serde
}

var _ = SerdeG[Change](&ChangeJSONSerdeG{})

func (s ChangeJSONSerdeG) Encode(value Change) ([]byte, error) {
	c, err := changeToChangeSer(value, s.ValJSONSerde)
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

func (s ChangeJSONSerdeG) Decode(value []byte) (Change, error) {
	val := ChangeSerialized{}
	if err := json.Unmarshal(value, &val); err != nil {
		return Change{}, err
	}
	return changeSerToChange(&val, s.ValJSONSerde)
}

type ChangeMsgpSerdeG struct {
	DefaultMsgpSerde
	ValMsgpSerde Serde
}

var _ = SerdeG[Change](ChangeMsgpSerdeG{})

func (s ChangeMsgpSerdeG) Encode(value Change) ([]byte, error) {
	c, err := changeToChangeSer(value, s.ValMsgpSerde)
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
