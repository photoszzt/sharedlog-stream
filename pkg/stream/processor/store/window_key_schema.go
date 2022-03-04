package store

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"sharedlog-stream/pkg/debug"
)

type WindowKeySchema struct{}

const (
	suffix_size = seqnum_size + ts_size
)

var (
	min_suffix = make([]byte, 0, suffix_size)
)

var _ = KeySchema(&WindowKeySchema{})

func (wks *WindowKeySchema) UpperRange(key []byte, to int64) []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, to)
	binary.Write(buffer, binary.BigEndian, int32(math.MaxInt32))
	fmt.Fprint(os.Stderr, "max suffix is\n")
	maxSuffix := buffer.Bytes()
	debug.PrintByteSlice(maxSuffix)
	return UpperRange(key, maxSuffix)
}

func (wks *WindowKeySchema) LowerRange(key []byte, from int64) []byte {
	return LowerRange(key, min_suffix)
}

func (wks *WindowKeySchema) ToStoreBinaryKeyPrefix(key []byte, ts int64) ([]byte, error) {
	panic("not supported")
}

func (wks *WindowKeySchema) UpperRangeFixedSize(key []byte, to int64) []byte {
	return wks.ToStoreKeyBinary(key, to, math.MaxInt32)
}

func (wks *WindowKeySchema) LowerRangeFixedSize(key []byte, from int64) []byte {
	ts := from
	if from < 0 {
		ts = 0
	}
	return wks.ToStoreKeyBinary(key, ts, 0)
}

func (wks *WindowKeySchema) SegmentTimestamp(key []byte) int64 {
	return wks.ExtractStoreTs(key)
}

func (wks *WindowKeySchema) HasNextCondition(curKey []byte, binaryKeyFrom []byte, binaryKeyTo []byte, from int64, to int64) (bool, int64) {
	keyBytes := wks.ExtractStoreKeyBytes(curKey)
	time := wks.ExtractStoreTs(curKey)
	if (binaryKeyFrom == nil || bytes.Compare(keyBytes, binaryKeyFrom) >= 0) &&
		(binaryKeyTo == nil || bytes.Compare(keyBytes, binaryKeyTo) <= 0) &&
		time >= from && time <= to {
		return true, time
	} else {
		return false, 0
	}
}

func (wks *WindowKeySchema) ToStoreKeyBinary(key []byte, ts int64, seqnum uint32) []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, key)
	binary.Write(buffer, binary.BigEndian, ts)
	binary.Write(buffer, binary.BigEndian, seqnum)
	return buffer.Bytes()
}

func (wks *WindowKeySchema) ExtractStoreTs(key []byte) int64 {
	return int64(binary.BigEndian.Uint64(key[len(key)-ts_size-seqnum_size:]))
}

func (wks *WindowKeySchema) ExtractStoreKeyBytes(key []byte) []byte {
	var buffer bytes.Buffer
	buffer.Write(key[0 : len(key)-ts_size-seqnum_size])
	return buffer.Bytes()
}
