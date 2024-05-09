package commtypes

import (
	"encoding/binary"
	"math"
	"testing"

	"golang.org/x/exp/constraints"
)

const float64EqualityThreshold = 1e-9

func almostEqual[V constraints.Float](a, b V) bool {
	return math.Abs(float64(a-b)) <= float64EqualityThreshold
}

func BenchmarkPooledSerdeUint64(b *testing.B) {
	a := uint64(100000)
	s := Uint64SerdeG{}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ret, buf, err := s.Encode(a)
		if err != nil {
			b.Fatal(err)
		}
		*buf = ret
		PushBuffer(buf)
	}
}

func BenchmarkPooledSerdeUint64_2(b *testing.B) {
	a := uint64(100000)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b := PopBuffer()
		bs := *b
		bs = Require(bs[:0], 8)
		bs = binary.BigEndian.AppendUint64(bs, a)
		*b = bs
		PushBuffer(b)
	}
}

func GenTestEncodeDecodeFloat[V constraints.Float](v V, t *testing.T, serdeG SerdeG[V], serde Serde) {
	bts, buf, err := serdeG.Encode(v)
	if err != nil {
		t.Fatal(err)
	}
	ret, err := serdeG.Decode(bts)
	if err != nil {
		t.Fatal(err)
	}
	if !almostEqual(v, ret) {
		t.Fatal("encode and decode doesn't give same value")
	}
	if serdeG.UsedBufferPool() {
		*buf = bts
		PushBuffer(buf)
	}

	bts, buf, err = serde.Encode(v)
	if err != nil {
		t.Fatal(err)
	}
	r, err := serde.Decode(bts)
	if err != nil {
		t.Fatal(err)
	}
	if !almostEqual(v, r.(V)) {
		t.Fatal("encode and decode doesn't give same value")
	}
	if serde.UsedBufferPool() {
		*buf = bts
		PushBuffer(buf)
	}
}

func GenTestEncodeDecode[V comparable](v V, t *testing.T, serdeG SerdeG[V], serde Serde) {
	bts, buf, err := serdeG.Encode(v)
	if err != nil {
		t.Fatal(err)
	}
	ret, err := serdeG.Decode(bts)
	if err != nil {
		t.Fatal(err)
	}
	if v != ret {
		t.Fatal("encode and decode doesn't give same value")
	}
	if serdeG.UsedBufferPool() {
		*buf = bts
		PushBuffer(buf)
	}

	bts, buf, err = serde.Encode(v)
	if err != nil {
		t.Fatal(err)
	}
	r, err := serde.Decode(bts)
	if err != nil {
		t.Fatal(err)
	}
	if v != r.(V) {
		t.Fatal("encode and decode doesn't give same value")
	}
	if serde.UsedBufferPool() {
		*buf = bts
		PushBuffer(buf)
	}
}

func TestEncodeDecodeFloat32(t *testing.T) {
	a := float32(0.25)
	s := Serde(Float32Serde{})
	ss := SerdeG[float32](Float32SerdeG{})
	GenTestEncodeDecodeFloat(a, t, ss, s)
}

func TestEncodeDecodeFloat64(t *testing.T) {
	a := float64(0.25)
	s := Serde(Float64Serde{})
	ss := SerdeG[float64](Float64SerdeG{})
	GenTestEncodeDecodeFloat(a, t, ss, s)
}

func TestEncodeDecodeUint64(t *testing.T) {
	a := uint64(100000)
	s := Serde(Uint64Serde{})
	ss := SerdeG[uint64](Uint64SerdeG{})
	GenTestEncodeDecode(a, t, ss, s)
}

func TestEncodeDecodeUint32(t *testing.T) {
	a := uint32(100000)
	s := Uint32Serde{}
	ss := Uint32SerdeG{}
	GenTestEncodeDecode[uint32](a, t, ss, s)
}

func TestEncodeDecodeUint16(t *testing.T) {
	a := uint16(1111)
	s := Uint16Serde{}
	ss := Uint16SerdeG{}
	GenTestEncodeDecode[uint16](a, t, ss, s)
}

func TestEncodeDecodeUint8(t *testing.T) {
	a := uint8(111)
	s := Uint8Serde{}
	ss := Uint8SerdeG{}
	GenTestEncodeDecode[uint8](a, t, ss, s)
}

func TestEncodeDecodeInt64(t *testing.T) {
	a := int64(-100000)
	s := Int64Serde{}
	ss := Int64SerdeG{}
	GenTestEncodeDecode[int64](a, t, ss, s)
}

func TestEncodeDecodeInt32(t *testing.T) {
	a := int32(-100000)
	s := Int32Serde{}
	ss := Int32SerdeG{}
	GenTestEncodeDecode[int32](a, t, ss, s)
}

func TestEncodeDecodeInt16(t *testing.T) {
	a := int16(-1111)
	s := Int16Serde{}
	ss := Int16SerdeG{}
	GenTestEncodeDecode[int16](a, t, ss, s)
}

func TestEncodeDecodeInt8(t *testing.T) {
	a := int8(-111)
	s := Int8Serde{}
	ss := Int8SerdeG{}
	GenTestEncodeDecode[int8](a, t, ss, s)
}
