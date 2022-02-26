package store

import (
	"bytes"
	"encoding/binary"
	"math"
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
	ts_buf := make([]byte, 0, ts_size)
	binary.LittleEndian.PutUint64(ts_buf, uint64(to))
	seq_buf := make([]byte, 0, seqnum_size)
	binary.LittleEndian.PutUint32(seq_buf, math.MaxInt32)

	buf := make([]byte, 0, suffix_size)
	buffer := bytes.NewBuffer(buf)
	buffer.Write(ts_buf)
	buffer.Write(seq_buf)
	maxSuffix := buffer.Bytes()
	return UpperRange(key, maxSuffix)
}

func (wks *WindowKeySchema) LowerRange(key []byte, from int64) []byte {
	return LowerRange(key, min_suffix)
}

func (wks *WindowKeySchema) ToStoreBinaryKeyPrefix(key []byte, ts int64) ([]byte, error) {
	panic("not supported")
}

func (wks *WindowKeySchema) UpperRangeFixedSize(key []byte, to int64) []byte {
	return wks.toStoreKeyBinary(key, to, math.MaxInt32)
}

func (wks *WindowKeySchema) LowerRangeFixedSize(key []byte, from int64) []byte {
	ts := from
	if from < 0 {
		ts = 0
	}
	return wks.toStoreKeyBinary(key, ts, 0)
}

func (wks *WindowKeySchema) SegmentTimestamp(key []byte) int64 {
	return wks.extractStoreTs(key)
}

func (wks *WindowKeySchema) HasNextCondition(binaryKeyFrom []byte, binaryKeyTo []byte, from int64, to int64) {

}

func (wks *WindowKeySchema) toStoreKeyBinary(key []byte, ts int64, seqnum uint32) []byte {
	buf := make([]byte, 0, ts_size+len(key)+seqnum_size)
	ts_buf := make([]byte, 0, ts_size)
	binary.LittleEndian.PutUint64(ts_buf, uint64(ts))
	seqnum_buf := make([]byte, 0, seqnum_size)
	binary.LittleEndian.PutUint32(seqnum_buf, uint32(seqnum))
	buffer := bytes.NewBuffer(buf)
	buffer.Write(key)
	buffer.Write(ts_buf)
	buffer.Write(seqnum_buf)
	return buffer.Bytes()
}

func (wks *WindowKeySchema) extractStoreTs(key []byte) int64 {
	return int64(binary.LittleEndian.Uint64(key[len(key)-ts_size-seqnum_size:]))
}
