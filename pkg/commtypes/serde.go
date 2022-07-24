//go:generate stringer -type=SerdeFormat
package commtypes

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

type SerdeFormat uint8

const (
	JSON SerdeFormat = 0
	MSGP SerdeFormat = 1
)

type Encoder interface {
	Encode(interface{}) ([]byte, error)
}

type EncoderG[V any] interface {
	Encode(v V) ([]byte, error)
}

type EncoderFunc func(interface{}) ([]byte, error)

func (ef EncoderFunc) Encode(v interface{}) ([]byte, error) {
	return ef(v)
}

type Decoder interface {
	Decode([]byte) (interface{}, error)
}

type DecoderG[V any] interface {
	Decode([]byte) (V, error)
}

type DecoderFunc func([]byte) (interface{}, error)

func (df DecoderFunc) Decode(b []byte) (interface{}, error) {
	return df(b)
}

type Serde interface {
	Encoder
	Decoder
}

type SerdeG[V any] interface {
	EncoderG[V]
	DecoderG[V]
}

type Float64Encoder struct{}
type Float64EncoderG struct{}

var _ = Encoder(Float64Encoder{})
var _ = EncoderG[float64](Float64EncoderG{})

func (e Float64Encoder) Encode(value interface{}) ([]byte, error) {
	if value == nil {
		return nil, nil
	}
	v := value.(float64)
	bits := math.Float64bits(v)
	bs := make([]byte, 8)
	binary.BigEndian.PutUint64(bs, bits)
	return bs, nil
}

func (e Float64EncoderG) Encode(value float64) ([]byte, error) {
	bits := math.Float64bits(value)
	bs := make([]byte, 8)
	binary.BigEndian.PutUint64(bs, bits)
	return bs, nil
}

type Float64Decoder struct{}
type Float64DecoderG struct{}

var _ = Decoder(Float64Decoder{})
var _ = DecoderG[float64](Float64DecoderG{})

func (e Float64Decoder) Decode(value []byte) (interface{}, error) {
	if value == nil {
		return nil, nil
	}
	if len(value) != 8 {
		return nil, sizeNot8
	}
	bits := binary.BigEndian.Uint64(value)
	return math.Float64frombits(bits), nil
}

func (e Float64DecoderG) Decode(value []byte) (float64, error) {
	if value == nil {
		return 0, nil
	}
	if len(value) != 8 {
		return 0, sizeNot8
	}
	bits := binary.BigEndian.Uint64(value)
	return math.Float64frombits(bits), nil
}

type Float64Serde struct {
	Float64Encoder
	Float64Decoder
}

type Float64SerdeG struct {
	Float64EncoderG
	Float64DecoderG
}

var _ = Serde(Float64Serde{})
var _ = SerdeG[float64](Float64SerdeG{})

type Float32Encoder struct{}
type Float32EncoderG struct{}

var _ = Encoder(Float32Encoder{})
var _ = EncoderG[float32](Float32EncoderG{})

func (e Float32Encoder) Encode(value interface{}) ([]byte, error) {
	if value == nil {
		return nil, nil
	}
	v := value.(float32)
	bits := math.Float32bits(v)
	bs := make([]byte, 4)
	binary.BigEndian.PutUint32(bs, bits)
	return bs, nil
}

func (e Float32EncoderG) Encode(value float32) ([]byte, error) {
	bits := math.Float32bits(value)
	bs := make([]byte, 4)
	binary.BigEndian.PutUint32(bs, bits)
	return bs, nil
}

type Float32Decoder struct{}
type Float32DecoderG struct{}

var _ = Decoder(Float32Decoder{})
var _ = DecoderG[float32](Float32DecoderG{})

func (e Float32Decoder) Decode(value []byte) (interface{}, error) {
	if value == nil {
		return nil, nil
	}
	if len(value) != 4 {
		return nil, sizeNot4
	}
	bits := binary.BigEndian.Uint32(value)
	return math.Float32frombits(bits), nil
}

func (e Float32DecoderG) Decode(value []byte) (float32, error) {
	if value == nil {
		return 0, nil
	}
	if len(value) != 4 {
		return 0, sizeNot4
	}
	bits := binary.BigEndian.Uint32(value)
	return math.Float32frombits(bits), nil
}

type Float32Serde struct {
	Float32Encoder
	Float32Decoder
}

type Float32SerdeG struct {
	Float32EncoderG
	Float32DecoderG
}

var _ = Serde(Float32Serde{})
var _ = SerdeG[float32](Float32SerdeG{})

type Uint64Encoder struct{}
type Uint64EncoderG struct{}

var _ = Encoder(Uint64Encoder{})
var _ = EncoderG[uint64](Uint64EncoderG{})

func (e Uint64Encoder) Encode(value interface{}) ([]byte, error) {
	if value == nil {
		return nil, nil
	}
	v := value.(uint64)
	bs := make([]byte, 8)
	binary.BigEndian.PutUint64(bs, v)
	return bs, nil
}

func (e Uint64EncoderG) Encode(value uint64) ([]byte, error) {
	bs := make([]byte, 8)
	binary.BigEndian.PutUint64(bs, value)
	return bs, nil
}

type Uint64Decoder struct{}
type Uint64DecoderG struct{}

var _ = Decoder(Uint64Decoder{})
var _ = DecoderG[uint64](Uint64DecoderG{})

func (d Uint64Decoder) Decode(value []byte) (interface{}, error) {
	if value == nil {
		return nil, nil
	}
	if len(value) != 8 {
		return nil, sizeNot8
	}
	bits := binary.BigEndian.Uint64(value)
	return bits, nil
}

func (d Uint64DecoderG) Decode(value []byte) (uint64, error) {
	if value == nil {
		return 0, nil
	}
	if len(value) != 8 {
		return 0, sizeNot8
	}
	bits := binary.BigEndian.Uint64(value)
	return bits, nil
}

type Uint64Serde struct {
	Uint64Encoder
	Uint64Decoder
}

type Uint64SerdeG struct {
	Uint64EncoderG
	Uint64DecoderG
}

var _ = Serde(Uint64Serde{})
var _ = SerdeG[uint64](Uint64SerdeG{})

type Int64Encoder struct{}
type Int64EncoderG struct{}

var _ = Encoder(Int64Encoder{})
var _ = EncoderG[int64](Int64EncoderG{})

func (e Int64Encoder) Encode(value interface{}) ([]byte, error) {
	if value == nil {
		return nil, nil
	}
	v := value.(int64)
	bs := make([]byte, 8)
	binary.BigEndian.PutUint64(bs, uint64(v))
	return bs, nil
}

func (e Int64EncoderG) Encode(value int64) ([]byte, error) {
	bs := make([]byte, 8)
	binary.BigEndian.PutUint64(bs, uint64(value))
	return bs, nil
}

type Int64Decoder struct{}
type Int64DecoderG struct{}

var _ = Decoder(Int64Decoder{})
var _ = DecoderG[int64](Int64DecoderG{})

func (d Int64Decoder) Decode(value []byte) (interface{}, error) {
	if value == nil {
		return nil, nil
	}
	if len(value) != 8 {
		return nil, sizeNot8
	}
	bits := binary.BigEndian.Uint64(value)
	return int64(bits), nil
}

func (d Int64DecoderG) Decode(value []byte) (int64, error) {
	if value == nil {
		return 0, nil
	}
	if len(value) != 8 {
		return 0, sizeNot8
	}
	bits := binary.BigEndian.Uint64(value)
	return int64(bits), nil
}

type Int64Serde struct {
	Int64Encoder
	Int64Decoder
}

type Int64SerdeG struct {
	Int64EncoderG
	Int64DecoderG
}

var _ = Serde(Int64Serde{})
var _ = SerdeG[int64](Int64SerdeG{})

type Uint32Encoder struct{}
type Uint32EncoderG struct{}

var _ = Encoder(Uint32Encoder{})
var _ = EncoderG[uint32](Uint32EncoderG{})

func (e Uint32Encoder) Encode(value interface{}) ([]byte, error) {
	if value == nil {
		return nil, nil
	}
	v := value.(uint32)
	bs := make([]byte, 4)
	binary.BigEndian.PutUint32(bs, v)
	return bs, nil
}

func (e Uint32EncoderG) Encode(value uint32) ([]byte, error) {
	bs := make([]byte, 4)
	binary.BigEndian.PutUint32(bs, value)
	return bs, nil
}

type Uint32Decoder struct{}
type Uint32DecoderG struct{}

var _ = Decoder(Uint32Decoder{})
var _ = DecoderG[uint32](Uint32DecoderG{})

func (e Uint32Decoder) Decode(value []byte) (interface{}, error) {
	if value == nil {
		return nil, nil
	}
	if len(value) != 4 {
		return nil, sizeNot4
	}
	bits := binary.BigEndian.Uint32(value)
	return uint32(bits), nil
}

func (e Uint32DecoderG) Decode(value []byte) (uint32, error) {
	if value == nil {
		return 0, nil
	}
	if len(value) != 4 {
		return 0, sizeNot4
	}
	bits := binary.BigEndian.Uint32(value)
	return uint32(bits), nil
}

type Uint32Serde struct {
	Uint32Encoder
	Uint32Decoder
}

type Uint32SerdeG struct {
	Uint32EncoderG
	Uint32DecoderG
}

var _ = Serde(Uint32Serde{})
var _ = SerdeG[uint32](Uint32SerdeG{})

type Int32Encoder struct{}
type Int32EncoderG struct{}

var _ = Encoder(Int32Encoder{})
var _ = EncoderG[int32](Int32EncoderG{})

func (e Int32Encoder) Encode(value interface{}) ([]byte, error) {
	if value == nil {
		return nil, nil
	}
	v := value.(int32)
	bs := make([]byte, 4)
	binary.BigEndian.PutUint32(bs, uint32(v))
	return bs, nil
}

func (e Int32EncoderG) Encode(value int32) ([]byte, error) {
	bs := make([]byte, 4)
	binary.BigEndian.PutUint32(bs, uint32(value))
	return bs, nil
}

type Int32Decoder struct{}
type Int32DecoderG struct{}

var _ = Decoder(Int32Decoder{})
var _ = DecoderG[int32](Int32DecoderG{})

func (e Int32Decoder) Decode(value []byte) (interface{}, error) {
	if value == nil {
		return nil, nil
	}
	if len(value) != 4 {
		return nil, sizeNot4
	}
	bits := binary.BigEndian.Uint32(value)
	return int32(bits), nil
}

func (e Int32DecoderG) Decode(value []byte) (int32, error) {
	if value == nil {
		return 0, nil
	}
	if len(value) != 4 {
		return 0, sizeNot4
	}
	bits := binary.BigEndian.Uint32(value)
	return int32(bits), nil
}

type Int32Serde struct {
	Int32Encoder
	Int32Decoder
}

type Int32SerdeG struct {
	Int32EncoderG
	Int32DecoderG
}

var _ = Serde(Int32Serde{})
var _ = SerdeG[int32](Int32SerdeG{})

type IntSerde struct{}
type IntSerdeG struct{}

var _ = SerdeG[int](IntSerdeG{})
var _ = Serde(IntSerde{})

func (s IntSerde) Encode(value interface{}) ([]byte, error) {
	if value == nil {
		return nil, nil
	}
	v := value.(int)
	bs := make([]byte, 4)
	binary.BigEndian.PutUint32(bs, uint32(v))
	return bs, nil
}

func (s IntSerdeG) Encode(value int) ([]byte, error) {
	bs := make([]byte, 4)
	binary.BigEndian.PutUint32(bs, uint32(value))
	return bs, nil
}

func (s IntSerde) Decode(value []byte) (interface{}, error) {
	if value == nil {
		return nil, nil
	}
	if len(value) != 4 {
		return nil, sizeNot4
	}
	bits := binary.BigEndian.Uint32(value)
	return int(bits), nil
}

func (s IntSerdeG) Decode(value []byte) (int, error) {
	if value == nil {
		return 0, nil
	}
	if len(value) != 4 {
		return 0, sizeNot4
	}
	bits := binary.BigEndian.Uint32(value)
	return int(bits), nil
}

type Uint16Encoder struct{}
type Uint16EncoderG struct{}

var _ = Encoder(Uint16Encoder{})
var _ = EncoderG[uint16](Uint16EncoderG{})

func (e Uint16Encoder) Encode(value interface{}) ([]byte, error) {
	if value == nil {
		return nil, nil
	}
	v := value.(uint16)
	bs := make([]byte, 2)
	binary.BigEndian.PutUint16(bs, v)
	return bs, nil
}

func (e Uint16EncoderG) Encode(value uint16) ([]byte, error) {
	bs := make([]byte, 2)
	binary.BigEndian.PutUint16(bs, value)
	return bs, nil
}

type Uint16Decoder struct{}
type Uint16DecoderG struct{}

var _ = Decoder(Uint16Decoder{})
var _ = DecoderG[uint16](Uint16DecoderG{})

func (e Uint16Decoder) Decode(value []byte) (interface{}, error) {
	if value == nil {
		return nil, nil
	}
	if len(value) != 2 {
		return nil, sizeNot2
	}
	bits := binary.BigEndian.Uint16(value)
	return bits, nil
}

func (e Uint16DecoderG) Decode(value []byte) (uint16, error) {
	if value == nil {
		return 0, nil
	}
	if len(value) != 2 {
		return 0, sizeNot2
	}
	bits := binary.BigEndian.Uint16(value)
	return bits, nil
}

type Uint16Serde struct {
	Uint16Encoder
	Uint16Decoder
}

type Uint16SerdeG struct {
	Uint16EncoderG
	Uint16DecoderG
}

var _ = Serde(Uint16Serde{})
var _ = SerdeG[uint16](Uint16SerdeG{})

//
type Int16Encoder struct{}
type Int16EncoderG struct{}

var _ = Encoder(Int16Encoder{})
var _ = EncoderG[int16](Int16EncoderG{})

func (e Int16Encoder) Encode(value interface{}) ([]byte, error) {
	if value == nil {
		return nil, nil
	}
	v := value.(int16)
	bs := make([]byte, 2)
	binary.BigEndian.PutUint16(bs, uint16(v))
	return bs, nil
}

func (e Int16EncoderG) Encode(value int16) ([]byte, error) {
	bs := make([]byte, 2)
	binary.BigEndian.PutUint16(bs, uint16(value))
	return bs, nil
}

type Int16Decoder struct{}
type Int16DecoderG struct{}

var _ = Decoder(Int16Decoder{})
var _ = DecoderG[int16](Int16DecoderG{})

func (e Int16Decoder) Decode(value []byte) (interface{}, error) {
	if value == nil {
		return nil, nil
	}
	if len(value) != 2 {
		return nil, sizeNot2
	}
	bits := binary.BigEndian.Uint16(value)
	return int16(bits), nil
}

func (e Int16DecoderG) Decode(value []byte) (int16, error) {
	if value == nil {
		return 0, nil
	}
	if len(value) != 2 {
		return 0, sizeNot2
	}
	bits := binary.BigEndian.Uint16(value)
	return int16(bits), nil
}

type Int16Serde struct {
	Int16Encoder
	Int16Decoder
}

type Int16SerdeG struct {
	Int16EncoderG
	Int16DecoderG
}

var _ = Serde(Int16Serde{})
var _ = SerdeG[int16](Int16SerdeG{})

type Uint8Encoder struct{}
type Uint8EncoderG struct{}

var _ = Encoder(Uint8Encoder{})
var _ = EncoderG[uint8](Uint8EncoderG{})

func (e Uint8Encoder) Encode(value interface{}) ([]byte, error) {
	if value == nil {
		return nil, nil
	}
	v := value.(uint8)
	bs := make([]byte, 1)
	bs[0] = v
	return bs, nil
}

func (e Uint8EncoderG) Encode(value uint8) ([]byte, error) {
	bs := make([]byte, 1)
	bs[0] = value
	return bs, nil
}

type Uint8Decoder struct{}
type Uint8DecoderG struct{}

var _ = Decoder(Uint8Decoder{})
var _ = DecoderG[uint8](Uint8DecoderG{})

func (e Uint8Decoder) Decode(value []byte) (interface{}, error) {
	if value == nil {
		return nil, nil
	}
	if len(value) != 1 {
		return nil, sizeNot1
	}
	return value[0], nil
}

func (e Uint8DecoderG) Decode(value []byte) (uint8, error) {
	if value == nil {
		return 0, nil
	}
	if len(value) != 1 {
		return 0, sizeNot1
	}
	return value[0], nil
}

type Uint8Serde struct {
	Uint8Encoder
	Uint8Decoder
}

type Uint8SerdeG struct {
	Uint8EncoderG
	Uint8DecoderG
}

var _ = Serde(Uint8Serde{})
var _ = SerdeG[uint8](Uint8SerdeG{})

//
type Int8Encoder struct{}
type Int8EncoderG struct{}

var _ = Encoder(Int8Encoder{})
var _ = EncoderG[int8](Int8EncoderG{})

func (e Int8Encoder) Encode(value interface{}) ([]byte, error) {
	if value == nil {
		return nil, nil
	}
	v := value.(int8)
	bs := make([]byte, 1)
	bs[0] = uint8(v)
	return bs, nil
}

func (e Int8EncoderG) Encode(value int8) ([]byte, error) {
	bs := make([]byte, 1)
	bs[0] = uint8(value)
	return bs, nil
}

type Int8Decoder struct{}
type Int8DecoderG struct{}

var _ = Decoder(Int8Decoder{})
var _ = DecoderG[int8](Int8DecoderG{})

func (e Int8Decoder) Decode(value []byte) (interface{}, error) {
	if value == nil {
		return nil, nil
	}
	if len(value) != 1 {
		return nil, sizeNot1
	}
	return int8(value[0]), nil
}

func (e Int8DecoderG) Decode(value []byte) (int8, error) {
	if value == nil {
		return 0, nil
	}
	if len(value) != 1 {
		return 0, sizeNot1
	}
	return int8(value[0]), nil
}

type Int8Serde struct {
	Int8Encoder
	Int8Decoder
}

type Int8SerdeG struct {
	Int8EncoderG
	Int8DecoderG
}

var _ = Serde(Int8Serde{})
var _ = SerdeG[int8](Int8SerdeG{})

type StringEncoder struct{}
type StringEncoderG struct{}

var _ = Encoder(StringEncoder{})
var _ = EncoderG[string](StringEncoderG{})

func (e StringEncoder) Encode(value interface{}) ([]byte, error) {
	if value == nil {
		return nil, nil
	}
	v := value.(string)
	return []byte(v), nil
}

func (e StringEncoderG) Encode(value string) ([]byte, error) {
	return []byte(value), nil
}

type StringDecoder struct{}
type StringDecoderG struct{}

var _ = Decoder(StringDecoder{})
var _ = DecoderG[string](StringDecoderG{})

func (d StringDecoder) Decode(value []byte) (interface{}, error) {
	if value == nil {
		return nil, nil
	}
	return string(value), nil
}

func (d StringDecoderG) Decode(value []byte) (string, error) {
	if value == nil {
		return "", nil
	}
	return string(value), nil
}

type StringSerde struct {
	StringEncoder
	StringDecoder
}

type StringSerdeG struct {
	StringEncoderG
	StringDecoderG
}

var _ = Serde(StringSerde{})
var _ = SerdeG[string](StringSerdeG{})
