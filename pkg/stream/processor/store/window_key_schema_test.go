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
