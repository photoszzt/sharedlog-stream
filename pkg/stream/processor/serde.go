package processor

import (
	"encoding/binary"
	"math"

	"golang.org/x/xerrors"
)

var (
	sizeNot8 = xerrors.New("size of value to deserialized is not 8")
	sizeNot4 = xerrors.New("size of value to deserialized is not 4")
	sizeNot2 = xerrors.New("size of value to deserialized is not 2")
	sizeNot1 = xerrors.New("size of value to deserialized is not 1")
)

type Encoder interface {
	Encode(interface{}) ([]byte, error)
}

type EncoderFunc func(interface{}) ([]byte, error)

func (f EncoderFunc) Encode(value interface{}) ([]byte, error) {
	return f(value)
}

type Decoder interface {
	Decode([]byte) (interface{}, error)
}

type DecoderFunc func([]byte) (interface{}, error)

func (f DecoderFunc) Decode(value []byte) (interface{}, error) {
	return f(value)
}

type Serde interface {
	Encoder
	Decoder
}

type MsgEncoder interface {
	Encode(key []byte, value []byte) ([]byte, error)
}

type MsgDecoder interface {
	Decode([]byte) ([]byte /* key */, []byte /* value */, error)
}

type MsgSerde interface {
	MsgEncoder
	MsgDecoder
}

type Float64Encoder struct{}

var _ = Encoder(Float64Encoder{})

func (e Float64Encoder) Encode(value interface{}) ([]byte, error) {
	if value == nil {
		return nil, nil
	}
	v := value.(float64)
	bits := math.Float64bits(v)
	bs := make([]byte, 8)
	binary.LittleEndian.PutUint64(bs, bits)
	return bs, nil
}

type Float64Decoder struct{}

var _ = Decoder(Float64Decoder{})

func (e Float64Decoder) Decode(value []byte) (interface{}, error) {
	if value == nil {
		return nil, nil
	}
	if len(value) != 8 {
		return nil, sizeNot8
	}
	bits := binary.LittleEndian.Uint64(value)
	return math.Float64frombits(bits), nil
}

type Float64Serde struct {
	Float64Encoder
	Float64Decoder
}

var _ = Serde(Float64Serde{})

type Float32Encoder struct{}

var _ = Encoder(Float32Encoder{})

func (e Float32Encoder) Encode(value interface{}) ([]byte, error) {
	if value == nil {
		return nil, nil
	}
	v := value.(float32)
	bits := math.Float32bits(v)
	bs := make([]byte, 4)
	binary.LittleEndian.PutUint32(bs, bits)
	return bs, nil
}

type Float32Decoder struct{}

var _ = Decoder(Float32Decoder{})

func (e Float32Decoder) Decode(value []byte) (interface{}, error) {
	if value == nil {
		return nil, nil
	}
	if len(value) != 4 {
		return nil, sizeNot4
	}
	bits := binary.LittleEndian.Uint32(value)
	return math.Float32frombits(bits), nil
}

type Float32Serde struct {
	Float32Encoder
	Float32Decoder
}

type Uint64Encoder struct{}

var _ = Encoder(Uint64Encoder{})

func (e Uint64Encoder) Encode(value interface{}) ([]byte, error) {
	if value == nil {
		return nil, nil
	}
	v := value.(uint64)
	bs := make([]byte, 8)
	binary.LittleEndian.PutUint64(bs, v)
	return bs, nil
}

type Uint64Decoder struct{}

var _ = Decoder(Uint64Decoder{})

func (d Uint64Decoder) Decode(value []byte) (interface{}, error) {
	if value == nil {
		return nil, nil
	}
	if len(value) != 8 {
		return nil, sizeNot8
	}
	bits := binary.LittleEndian.Uint64(value)
	return bits, nil
}

type Uint64Serde struct {
	Uint64Encoder
	Uint64Decoder
}

type Int64Encoder struct{}

var _ = Encoder(Int64Encoder{})

func (e Int64Encoder) Encode(value interface{}) ([]byte, error) {
	if value == nil {
		return nil, nil
	}
	v := value.(int64)
	bs := make([]byte, 8)
	binary.LittleEndian.PutUint64(bs, uint64(v))
	return bs, nil
}

type Int64Decoder struct{}

var _ = Decoder(Uint64Decoder{})

func (d Int64Decoder) Decode(value []byte) (interface{}, error) {
	if value == nil {
		return nil, nil
	}
	if len(value) != 8 {
		return nil, sizeNot8
	}
	bits := binary.LittleEndian.Uint64(value)
	return int64(bits), nil
}

type Int64Serde struct {
	Int64Encoder
	Int64Decoder
}

type Uint32Encoder struct{}

var _ = Encoder(Uint32Encoder{})

func (e Uint32Encoder) Encode(value interface{}) ([]byte, error) {
	if value == nil {
		return nil, nil
	}
	v := value.(uint32)
	bs := make([]byte, 4)
	binary.LittleEndian.PutUint32(bs, v)
	return bs, nil
}

type Uint32Decoder struct{}

var _ = Decoder(Uint32Decoder{})

func (e Uint32Decoder) Decode(value []byte) (interface{}, error) {
	if value == nil {
		return nil, nil
	}
	if len(value) != 4 {
		return nil, sizeNot4
	}
	bits := binary.LittleEndian.Uint32(value)
	return uint32(bits), nil
}

type Uint32Serde struct {
	Uint32Encoder
	Uint32Decoder
}

type Int32Encoder struct{}

var _ = Encoder(Int32Encoder{})

func (e Int32Encoder) Encode(value interface{}) ([]byte, error) {
	if value == nil {
		return nil, nil
	}
	v := value.(int32)
	bs := make([]byte, 4)
	binary.LittleEndian.PutUint32(bs, uint32(v))
	return bs, nil
}

type Int32Decoder struct{}

var _ = Decoder(Int32Decoder{})

func (e Int32Decoder) Decode(value []byte) (interface{}, error) {
	if value == nil {
		return nil, nil
	}
	if len(value) != 4 {
		return nil, sizeNot4
	}
	bits := binary.LittleEndian.Uint32(value)
	return int32(bits), nil
}

type Int32Serde struct {
	Int32Encoder
	Int32Decoder
}

type Uint16Encoder struct{}

var _ = Encoder(Uint16Encoder{})

func (e Uint16Encoder) Encode(value interface{}) ([]byte, error) {
	if value == nil {
		return nil, nil
	}
	v := value.(uint16)
	bs := make([]byte, 2)
	binary.LittleEndian.PutUint16(bs, v)
	return bs, nil
}

type Uint16Decoder struct{}

var _ = Decoder(Uint16Decoder{})

func (e Uint16Decoder) Decode(value []byte) (interface{}, error) {
	if value == nil {
		return nil, nil
	}
	if len(value) != 2 {
		return nil, sizeNot2
	}
	bits := binary.LittleEndian.Uint16(value)
	return bits, nil
}

type Uint16Serde struct {
	Uint16Encoder
	Uint16Decoder
}

//
type Int16Encoder struct{}

var _ = Encoder(Uint16Encoder{})

func (e Int16Encoder) Encode(value interface{}) ([]byte, error) {
	if value == nil {
		return nil, nil
	}
	v := value.(int16)
	bs := make([]byte, 2)
	binary.LittleEndian.PutUint16(bs, uint16(v))
	return bs, nil
}

type Int16Decoder struct{}

var _ = Decoder(Uint16Decoder{})

func (e Int16Decoder) Decode(value []byte) (interface{}, error) {
	if value == nil {
		return nil, nil
	}
	if len(value) != 2 {
		return nil, sizeNot2
	}
	bits := binary.LittleEndian.Uint16(value)
	return bits, nil
}

type Int16Serde struct {
	Int16Encoder
	Int16Decoder
}

type Uint8Encoder struct{}

var _ = Encoder(Uint8Encoder{})

func (e Uint8Encoder) Encode(value interface{}) ([]byte, error) {
	if value == nil {
		return nil, nil
	}
	v := value.(uint8)
	bs := make([]byte, 1)
	bs[0] = v
	return bs, nil
}

type Uint8Decoder struct{}

var _ = Decoder(Uint8Decoder{})

func (e Uint8Decoder) Decode(value []byte) (interface{}, error) {
	if value == nil {
		return nil, nil
	}
	if len(value) != 1 {
		return nil, sizeNot1
	}
	return value[0], nil
}

type Uint8Serde struct {
	Uint8Encoder
	Uint8Decoder
}

//
type Int8Encoder struct{}

var _ = Encoder(Uint8Encoder{})

func (e Int8Encoder) Encode(value interface{}) ([]byte, error) {
	if value == nil {
		return nil, nil
	}
	v := value.(int8)
	bs := make([]byte, 1)
	bs[0] = uint8(v)
	return bs, nil
}

type Int8Decoder struct{}

var _ = Decoder(Uint8Decoder{})

func (e Int8Decoder) Decode(value []byte) (interface{}, error) {
	if value == nil {
		return nil, nil
	}
	if len(value) != 1 {
		return nil, sizeNot1
	}
	return int8(value[0]), nil
}

type Int8Serde struct {
	Int8Encoder
	Int8Decoder
}

type StringEncoder struct{}

var _ = Encoder(StringEncoder{})

func (e StringEncoder) Encode(value interface{}) ([]byte, error) {
	if value == nil {
		return nil, nil
	}
	v := value.(string)
	return []byte(v), nil
}

type StringDecoder struct{}

var _ = Decoder(StringDecoder{})

func (d StringDecoder) Decode(value []byte) (interface{}, error) {
	if value == nil {
		return nil, nil
	}
	return string(value), nil
}

type StringSerde struct {
	StringEncoder
	StringDecoder
}
