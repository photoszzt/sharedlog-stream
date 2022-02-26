package store

import "bytes"

const (
	min_key_length = 1
)

// Returns the upper byte range for a key with a given fixed size maximum suffix
// Assumes the minimum key length is one byte
func UpperRange(key []byte, maxSuffix []byte) []byte {
	buf := make([]byte, 0, len(key)+len(maxSuffix))
	buffer := bytes.NewBuffer(buf)

	i := 0
	for i < len(key) && (i < min_key_length || // assumes keys are at least one byte long
		(key[i]&0xFF) >= (maxSuffix[i]&0xFF)) {
		_ = buffer.WriteByte(key[i])
		i += 1
	}
	_, _ = buffer.Write(maxSuffix)
	rangeEnd := buffer.Bytes()
	remain := buffer.Cap() - buffer.Len()

	res := make([]byte, remain)
	buf_res := bytes.NewBuffer(res)
	_, _ = buf_res.Write(rangeEnd)
	return buf_res.Bytes()
}

func LowerRange(key []byte, minSuffix []byte) []byte {
	buf := make([]byte, 0, len(key)+len(minSuffix))
	buffer := bytes.NewBuffer(buf)
	// any key in the range would start at least with the given prefix to be
	// in the range, and have at least SUFFIX_SIZE number of trailing zero bytes.

	// unless there is a maximum key length, you can keep appending more zero bytes
	// to keyFrom to create a key that will match the range, yet that would precede
	// KeySchema.toBinaryKey(keyFrom, from, 0) in byte order
	buffer.Write(key)
	buffer.Write(minSuffix)
	return buffer.Bytes()
}
