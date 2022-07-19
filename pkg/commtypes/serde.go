//go:generate stringer -type=SerdeFormat
package commtypes

import (
	"encoding/binary"
	"math"

	"golang.org/x/xerrors"
)

var (
	SizeNot8 = xerrors.New("size of value to deserialized is not 8")
	SizeNot4 = xerrors.New("size of value to deserialized is not 4")
	SizeNot2 = xerrors.New("size of value to deserialized is not 2")
	SizeNot1 = xerrors.New("size of value to deserialized is not 1")
)

type SerdeFormat uint8

const (
	JSON SerdeFormat = 0
	MSGP SerdeFormat = 1
)

type Encoder[T any] interface {
	Encode(T) ([]byte, error)
}

type EncoderFunc func(interface{}) ([]byte, error)

func (f EncoderFunc) Encode(value interface{}) ([]byte, error) {
	return f(value)
}

type Decoder[T any] interface {
	Decode([]byte) (T, error)
}

type DecoderFunc func([]byte) (interface{}, error)

func (f DecoderFunc) Decode(value []byte) (interface{}, error) {
	return f(value)
}

type Serde[T any] interface {
	Encoder[T]
	Decoder[T]
}

type Float64Encoder struct{}

var _ = Encoder[float64](Float64Encoder{})

func (e Float64Encoder) Encode(value float64) ([]byte, error) {
	bits := math.Float64bits(value)
	bs := make([]byte, 8)
	binary.BigEndian.PutUint64(bs, bits)
	return bs, nil
}

type Float64Decoder struct{}

var _ = Decoder[float64](Float64Decoder{})

func (e Float64Decoder) Decode(value []byte) (float64, error) {
	if value == nil {
		return 0, nil
	}
	if len(value) != 8 {
		return 0, SizeNot8
	}
	bits := binary.BigEndian.Uint64(value)
	return math.Float64frombits(bits), nil
}

type Float64Serde struct {
	Float64Encoder
	Float64Decoder
}

var _ = Serde[float64](Float64Serde{})

type Float32Encoder struct{}

var _ = Encoder[float32](Float32Encoder{})

func (e Float32Encoder) Encode(value float32) ([]byte, error) {
	bits := math.Float32bits(value)
	bs := make([]byte, 4)
	binary.BigEndian.PutUint32(bs, bits)
	return bs, nil
}

type Float32Decoder struct{}

var _ = Decoder[float32](Float32Decoder{})

func (e Float32Decoder) Decode(value []byte) (float32, error) {
	if value == nil {
		return 0, nil
	}
	if len(value) != 4 {
		return 0, SizeNot4
	}
	bits := binary.BigEndian.Uint32(value)
	return math.Float32frombits(bits), nil
}

type Float32Serde struct {
	Float32Encoder
	Float32Decoder
}

type Uint64Encoder struct{}

var _ = Encoder[uint64](Uint64Encoder{})

func (e Uint64Encoder) Encode(value uint64) ([]byte, error) {
	bs := make([]byte, 8)
	binary.BigEndian.PutUint64(bs, value)
	return bs, nil
}

type Uint64Decoder struct{}

var _ = Decoder[uint64](Uint64Decoder{})

func (d Uint64Decoder) Decode(value []byte) (uint64, error) {
	if value == nil {
		return 0, nil
	}
	if len(value) != 8 {
		return 0, SizeNot8
	}
	bits := binary.BigEndian.Uint64(value)
	return bits, nil
}

type Uint64Serde struct {
	Uint64Encoder
	Uint64Decoder
}

type Int64Encoder struct{}

var _ = Encoder[int64](Int64Encoder{})

func (e Int64Encoder) Encode(value int64) ([]byte, error) {
	bs := make([]byte, 8)
	binary.BigEndian.PutUint64(bs, uint64(value))
	return bs, nil
}

type Int64Decoder struct{}

var _ = Decoder[int64](Int64Decoder{})

func (d Int64Decoder) Decode(value []byte) (int64, error) {
	if value == nil {
		return 0, nil
	}
	if len(value) != 8 {
		return 0, SizeNot8
	}
	bits := binary.BigEndian.Uint64(value)
	return int64(bits), nil
}

type Int64Serde struct {
	Int64Encoder
	Int64Decoder
}

type Uint32Encoder struct{}

var _ = Encoder[uint32](Uint32Encoder{})

func (e Uint32Encoder) Encode(value uint32) ([]byte, error) {
	bs := make([]byte, 4)
	binary.BigEndian.PutUint32(bs, value)
	return bs, nil
}

type Uint32Decoder struct{}

var _ = Decoder[uint32](Uint32Decoder{})

func (e Uint32Decoder) Decode(value []byte) (uint32, error) {
	if value == nil {
		return 0, nil
	}
	if len(value) != 4 {
		return 0, SizeNot4
	}
	bits := binary.BigEndian.Uint32(value)
	return uint32(bits), nil
}

type Uint32Serde struct {
	Uint32Encoder
	Uint32Decoder
}

type Int32Encoder struct{}

var _ = Encoder[int32](Int32Encoder{})

func (e Int32Encoder) Encode(value int32) ([]byte, error) {
	bs := make([]byte, 4)
	binary.BigEndian.PutUint32(bs, uint32(value))
	return bs, nil
}

type Int32Decoder struct{}

var _ = Decoder[int32](Int32Decoder{})

func (e Int32Decoder) Decode(value []byte) (int32, error) {
	if value == nil {
		return 0, nil
	}
	if len(value) != 4 {
		return 0, SizeNot4
	}
	bits := binary.BigEndian.Uint32(value)
	return int32(bits), nil
}

type Int32Serde struct {
	Int32Encoder
	Int32Decoder
}

type IntSerde struct{}

var _ = Serde[int](IntSerde{})

func (s IntSerde) Encode(value int) ([]byte, error) {
	bs := make([]byte, 4)
	binary.BigEndian.PutUint32(bs, uint32(value))
	return bs, nil
}

func (s IntSerde) Decode(value []byte) (int, error) {
	if value == nil {
		return 0, nil
	}
	if len(value) != 4 {
		return 0, SizeNot4
	}
	bits := binary.BigEndian.Uint32(value)
	return int(bits), nil
}

type Uint16Encoder struct{}

var _ = Encoder[uint16](Uint16Encoder{})

func (e Uint16Encoder) Encode(value uint16) ([]byte, error) {
	bs := make([]byte, 2)
	binary.BigEndian.PutUint16(bs, value)
	return bs, nil
}

type Uint16Decoder struct{}

var _ = Decoder[uint16](Uint16Decoder{})

func (e Uint16Decoder) Decode(value []byte) (uint16, error) {
	if value == nil {
		return 0, nil
	}
	if len(value) != 2 {
		return 0, SizeNot2
	}
	bits := binary.BigEndian.Uint16(value)
	return bits, nil
}

type Uint16Serde struct {
	Uint16Encoder
	Uint16Decoder
}

//
type Int16Encoder struct{}

var _ = Encoder[int16](Int16Encoder{})

func (e Int16Encoder) Encode(value int16) ([]byte, error) {
	bs := make([]byte, 2)
	binary.BigEndian.PutUint16(bs, uint16(value))
	return bs, nil
}

type Int16Decoder struct{}

var _ = Decoder[int16](Int16Decoder{})

func (e Int16Decoder) Decode(value []byte) (int16, error) {
	if value == nil {
		return 0, nil
	}
	if len(value) != 2 {
		return 0, SizeNot2
	}
	bits := binary.BigEndian.Uint16(value)
	return int16(bits), nil
}

type Int16Serde struct {
	Int16Encoder
	Int16Decoder
}

type Uint8Encoder struct{}

var _ = Encoder[uint8](Uint8Encoder{})

func (e Uint8Encoder) Encode(value uint8) ([]byte, error) {
	bs := make([]byte, 1)
	bs[0] = value
	return bs, nil
}

type Uint8Decoder struct{}

var _ = Decoder[uint8](Uint8Decoder{})

func (e Uint8Decoder) Decode(value []byte) (uint8, error) {
	if value == nil {
		return 0, nil
	}
	if len(value) != 1 {
		return 0, SizeNot1
	}
	return value[0], nil
}

type Uint8Serde struct {
	Uint8Encoder
	Uint8Decoder
}

//
type Int8Encoder struct{}

var _ = Encoder[int8](Int8Encoder{})

func (e Int8Encoder) Encode(value int8) ([]byte, error) {
	bs := make([]byte, 1)
	bs[0] = uint8(value)
	return bs, nil
}

type Int8Decoder struct{}

var _ = Decoder[int8](Int8Decoder{})

func (e Int8Decoder) Decode(value []byte) (int8, error) {
	if value == nil {
		return 0, nil
	}
	if len(value) != 1 {
		return 0, SizeNot1
	}
	return int8(value[0]), nil
}

type Int8Serde struct {
	Int8Encoder
	Int8Decoder
}

type StringEncoder struct{}

var _ = Encoder[string](StringEncoder{})

func (e StringEncoder) Encode(value string) ([]byte, error) {
	return []byte(value), nil
}

type StringDecoder struct{}

var _ = Decoder[string](StringDecoder{})

func (d StringDecoder) Decode(value []byte) (string, error) {
	if value == nil {
		return "", nil
	}
	return string(value), nil
}

type StringSerde struct {
	StringEncoder
	StringDecoder
}
