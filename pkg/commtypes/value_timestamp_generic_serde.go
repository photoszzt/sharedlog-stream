package commtypes

import (
	"encoding/json"
	"fmt"
	"sharedlog-stream/pkg/utils"

	"4d63.com/optional"
)

type ValueTimestampJSONSerdeG struct {
	ValJSONSerde Serde
}

var _ = SerdeG[*ValueTimestamp](ValueTimestampJSONSerdeG{})

func (s ValueTimestampJSONSerdeG) Encode(value *ValueTimestamp) ([]byte, error) {
	vs, err := valTsToValueTsSer(value, s.ValJSONSerde)
	if err != nil {
		return nil, err
	}
	if vs == nil {
		return nil, nil
	}
	return json.Marshal(vs)
}

func (s ValueTimestampJSONSerdeG) Decode(value []byte) (*ValueTimestamp, error) {
	if value == nil {
		return nil, nil
	}
	vs := ValueTimestampSerialized{}
	if err := json.Unmarshal(value, &vs); err != nil {
		return nil, err
	}
	return valTsSerToValueTs(&vs, s.ValJSONSerde)
}

type ValueTimestampMsgpSerdeG struct {
	ValMsgpSerde Serde
}

func (s ValueTimestampMsgpSerdeG) Encode(value *ValueTimestamp) ([]byte, error) {
	vs, err := valTsToValueTsSer(value, s.ValMsgpSerde)
	if err != nil {
		return nil, err
	}
	if vs == nil {
		return nil, nil
	}
	return vs.MarshalMsg(nil)
}

func (s ValueTimestampMsgpSerdeG) Decode(value []byte) (*ValueTimestamp, error) {
	if value == nil {
		return nil, nil
	}
	vs := ValueTimestampSerialized{}
	_, err := vs.UnmarshalMsg(value)
	if err != nil {
		return nil, err
	}
	return valTsSerToValueTs(&vs, s.ValMsgpSerde)
}

func GetValueTsSerdeG(serdeFormat SerdeFormat, valSerde Serde) (SerdeG[*ValueTimestamp], error) {
	if serdeFormat == JSON {
		return ValueTimestampJSONSerdeG{
			ValJSONSerde: valSerde,
		}, nil
	} else if serdeFormat == MSGP {
		return ValueTimestampMsgpSerdeG{
			ValMsgpSerde: valSerde,
		}, nil
	} else {
		return nil, fmt.Errorf("serde format should be either json or msgp; but %v is given", serdeFormat)
	}
}

func CreateValueTimestampOptional[V any](val optional.Optional[V], ts int64) optional.Optional[*ValueTimestamp] {
	v, hasV := val.Get()
	if hasV {
		return optional.Of(&ValueTimestamp{
			Value:     v,
			Timestamp: ts,
		})
	} else {
		return optional.Empty[*ValueTimestamp]()
	}
}

func CreateValueTimestampOptionalWithIntrVal(val interface{}, ts int64) optional.Optional[*ValueTimestamp] {
	if utils.IsNil(val) {
		return optional.Empty[*ValueTimestamp]()
	} else {
		return optional.Of(&ValueTimestamp{
			Value:     val,
			Timestamp: ts,
		})
	}
}
