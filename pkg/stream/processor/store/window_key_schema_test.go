package store

import (
	"bytes"
	"fmt"
	"math"
	"os"
	"sharedlog-stream/pkg/debug"
	"testing"
)

func TestUpperBoundWithLargeTimestamps(t *testing.T) {
	wks := &WindowKeySchema{}
	upper := wks.UpperRange([]byte{0xA, 0xB, 0xC}, math.MaxInt64)
	shorter := wks.ToStoreKeyBinary([]byte{0xA}, math.MaxInt64, math.MaxInt32)
	ret := bytes.Compare(upper, shorter)
	fmt.Fprintf(os.Stderr, "compare out: %d\n", ret)
	if ret < 0 {
		fmt.Fprint(os.Stderr, "upper: \n")
		debug.PrintByteSlice(upper)
		fmt.Fprintf(os.Stderr, "len: %v\nshort: ", len(upper))
		debug.PrintByteSlice(shorter)
		fmt.Fprintf(os.Stderr, "len: %v\n", len(shorter))
		t.Fatal("shorter key with max ts should be in range")
	}

	shorter = wks.ToStoreKeyBinary([]byte{0xA, 0xB}, math.MaxInt64, math.MaxInt32)
	if ret := bytes.Compare(upper, shorter); ret < 0 {
		t.Fatal("shorter key with max ts should be in range")
	}

	eq := wks.ToStoreKeyBinary([]byte{0xA}, math.MaxInt64, math.MaxInt32)
	if ret := bytes.Compare(upper, eq); ret != 0 {
		t.Fatal("should be equal")
	}
}

func TestUpperBoundWithKeyBytesLargerThanFirstTsByte(t *testing.T) {
	wks := &WindowKeySchema{}
	upper := wks.UpperRange([]byte{0xA, 0x8F, 0x9F}, math.MaxInt64)
	shorter := wks.ToStoreKeyBinary([]byte{0xA, 0x8F}, math.MaxInt64, math.MaxInt32)
	if ret := bytes.Compare(upper, shorter); ret < 0 {
		fmt.Fprint(os.Stderr, "upper: \n")
		debug.PrintByteSlice(upper)
		fmt.Fprintf(os.Stderr, "len: %v\nshort: ", len(upper))
		debug.PrintByteSlice(shorter)
		fmt.Fprintf(os.Stderr, "len: %v\n", len(shorter))
		t.Fatal("shorter key with max ts should be in range")
	}
	eq := wks.ToStoreKeyBinary([]byte{0xA, 0x8F, 0x9F}, math.MaxInt64, math.MaxInt32)
	if ret := bytes.Compare(upper, eq); ret != 0 {
		t.Fatal("should be equal")
	}
}

func TestUpperBoundWithKeyBytesLargerAndSmallerThanFirstTimestampByte(t *testing.T) {
	wks := &WindowKeySchema{}
	upper := wks.UpperRange([]byte{0xC, 0xC, 0x9}, 0x0Affffffffffffff)
	shorter := wks.ToStoreKeyBinary(
		[]byte{0xC, 0xC},
		0x0Affffffffffffff,
		math.MaxInt32,
	)
	if ret := bytes.Compare(upper, shorter); ret < 0 {
		fmt.Fprint(os.Stderr, "TestUpperBoundWithKeyBytesLargerAndSmallerThanFirstTimestampByte\n")
		fmt.Fprint(os.Stderr, "upper: \n")
		debug.PrintByteSlice(upper)
		fmt.Fprintf(os.Stderr, "len: %v\nshort: ", len(upper))
		debug.PrintByteSlice(shorter)
		fmt.Fprintf(os.Stderr, "len: %v\n", len(shorter))
		t.Fatal("shorter key with max ts should be in range")
	}
	eq := wks.ToStoreKeyBinary([]byte{0xC, 0xC}, 0x0Affffffffffffff, math.MaxInt32)
	if ret := bytes.Compare(upper, eq); ret != 0 {
		t.Fatal("should be equal")
	}
}

func TestUpperBoundWithZeroTimestamp(t *testing.T) {
	wks := &WindowKeySchema{}
	upper := wks.UpperRange([]byte{0xA, 0xB, 0xC}, 0)
	eq := wks.ToStoreKeyBinary([]byte{0xA, 0xB, 0xC}, 0, math.MaxInt32)
	if ret := bytes.Compare(upper, eq); ret != 0 {
		t.Fatal("should be equal")
	}
}

func TestLowerBoundWithZeroTimestamp(t *testing.T) {
	wks := &WindowKeySchema{}
	lower := wks.LowerRange([]byte{0xA, 0xB, 0xC}, 0)
	eq := wks.ToStoreKeyBinary([]byte{0xA, 0xB, 0xC}, 0, 0)
	if ret := bytes.Compare(lower, eq); ret != 0 {
		fmt.Fprint(os.Stderr, "TestLowerBoundWithZeroTimestamp\nlower: \n")
		debug.PrintByteSlice(lower)
		fmt.Fprintf(os.Stderr, "len: %d\neq: ", len(lower))
		debug.PrintByteSlice(eq)
		fmt.Fprintf(os.Stderr, "len: %d\n", len(eq))
		t.Fatal("should be equal")
	}
}

func TestLowerBoundWithMonZeroTimestamp(t *testing.T) {
	wks := &WindowKeySchema{}
	lower := wks.LowerRange([]byte{0xA, 0xB, 0xC}, 42)
	eq := wks.ToStoreKeyBinary([]byte{0xA, 0xB, 0xC}, 0, 0)
	if ret := bytes.Compare(lower, eq); ret != 0 {
		t.Fatal("should be equal")
	}
}

func TestLowerBoundMatchesTrailingZeros(t *testing.T) {
	wks := &WindowKeySchema{}
	lower := wks.LowerRange([]byte{0xA, 0xB, 0xC}, math.MaxInt64-1)
	low := wks.ToStoreKeyBinary([]byte{0xA, 0xB, 0xC, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, math.MaxInt64-1, 0)
	if ret := bytes.Compare(lower, low); ret >= 0 {
		t.Fatal("should be lower")
	}
	eq := wks.ToStoreKeyBinary([]byte{0xA, 0xB, 0xC}, 0, 0)
	if ret := bytes.Compare(lower, eq); ret != 0 {
		t.Fatal("should be equal")
	}
}
