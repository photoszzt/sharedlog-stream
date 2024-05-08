package commtypes

import (
	"encoding/binary"
	"math"
	"testing"
)

const float64EqualityThreshold = 1e-9

func almostEqualFloat32(a, b float32) bool {
	return math.Abs(float64(a-b)) <= float64EqualityThreshold
}

func almostEqualFloat64(a, b float64) bool {
	return math.Abs(a-b) <= float64EqualityThreshold
}

func BenchmarkPooledSerdeUint64(b *testing.B) {
	a := uint64(100000)
	s := Uint64SerdeG{}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ret, err := s.Encode(a)
		if err != nil {
			b.Fatal(err)
		}
		*s.BufPtr = ret
		PushBuffer(s.BufPtr)
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

func TestEncodeDecodeFloat32(t *testing.T) {
	a := float32(0.25)
	s := Float32Serde{}
	b, err := s.Encode(a)
	if err != nil {
		t.Fatal(err)
	}
	ret, err := s.Decode(b)
	if err != nil {
		t.Fatal(err)
	}
	if !almostEqualFloat32(a, ret.(float32)) {
		t.Fatal("encode and decode doesn't give same value")
	}

	ss := Float32SerdeG{}
	b, err = ss.Encode(a)
	if err != nil {
		t.Fatal(err)
	}
	ret1, err := ss.Decode(b)
	if err != nil {
		t.Fatal(err)
	}
	if !almostEqualFloat32(a, ret1) {
		t.Fatal("encode and decode doesn't give same value")
	}
}

func TestEncodeDecodeFloat64(t *testing.T) {
	a := float64(0.25)
	s := Float64Serde{}
	b, err := s.Encode(a)
	if err != nil {
		t.Fatal(err)
	}
	ret, err := s.Decode(b)
	if err != nil {
		t.Fatal(err)
	}
	if !almostEqualFloat64(a, ret.(float64)) {
		t.Fatal("encode and decode doesn't give same value")
	}

	ss := Float64SerdeG{}
	b, err = ss.Encode(a)
	if err != nil {
		t.Fatal(err)
	}
	ret1, err := ss.Decode(b)
	if err != nil {
		t.Fatal(err)
	}
	if !almostEqualFloat64(a, ret1) {
		t.Fatal("encode and decode doesn't give same value")
	}
}

func TestEncodeDecodeUint64(t *testing.T) {
	a := uint64(100000)
	s := Uint64Serde{}
	b, err := s.Encode(a)
	if err != nil {
		t.Fatal(err)
	}
	ret, err := s.Decode(b)
	if err != nil {
		t.Fatal(err)
	}
	if a != ret.(uint64) {
		t.Fatal("encode and decode doesn't give same value")
	}

	ss := Uint64SerdeG{}
	b, err = ss.Encode(a)
	if err != nil {
		t.Fatal(err)
	}
	ret1, err := ss.Decode(b)
	if err != nil {
		t.Fatal(err)
	}
	if a != ret1 {
		t.Fatal("encode and decode doesn't give same value")
	}
}

func TestEncodeDecodeUint32(t *testing.T) {
	a := uint32(100000)
	s := Uint32Serde{}
	b, err := s.Encode(a)
	if err != nil {
		t.Fatal(err)
	}
	ret, err := s.Decode(b)
	if err != nil {
		t.Fatal(err)
	}
	if a != ret.(uint32) {
		t.Fatal("encode and decode doesn't give same value")
	}

	ss := Uint32SerdeG{}
	b, err = ss.Encode(a)
	if err != nil {
		t.Fatal(err)
	}
	ret1, err := ss.Decode(b)
	if err != nil {
		t.Fatal(err)
	}
	if a != ret1 {
		t.Fatal("encode and decode doesn't give same value")
	}
}

func TestEncodeDecodeUint16(t *testing.T) {
	a := uint16(1111)
	s := Uint16Serde{}
	b, err := s.Encode(a)
	if err != nil {
		t.Fatal(err)
	}
	ret, err := s.Decode(b)
	if err != nil {
		t.Fatal(err)
	}
	if a != ret.(uint16) {
		t.Fatal("encode and decode doesn't give same value")
	}

	ss := Uint16SerdeG{}
	b, err = ss.Encode(a)
	if err != nil {
		t.Fatal(err)
	}
	ret1, err := ss.Decode(b)
	if err != nil {
		t.Fatal(err)
	}
	if a != ret1 {
		t.Fatal("encode and decode doesn't give same value")
	}
}

func TestEncodeDecodeUint8(t *testing.T) {
	a := uint8(111)
	s := Uint8Serde{}
	b, err := s.Encode(a)
	if err != nil {
		t.Fatal(err)
	}
	ret, err := s.Decode(b)
	if err != nil {
		t.Fatal(err)
	}
	if a != ret.(uint8) {
		t.Fatal("encode and decode doesn't give same value")
	}

	ss := Uint8SerdeG{}
	b, err = ss.Encode(a)
	if err != nil {
		t.Fatal(err)
	}
	ret1, err := ss.Decode(b)
	if err != nil {
		t.Fatal(err)
	}
	if a != ret1 {
		t.Fatal("encode and decode doesn't give same value")
	}
}
