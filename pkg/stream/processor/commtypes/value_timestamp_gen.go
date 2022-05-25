package commtypes

// Code generated by github.com/tinylib/msgp DO NOT EDIT.

import (
	"github.com/tinylib/msgp/msgp"
)

// DecodeMsg implements msgp.Decodable
func (z *ValueTimestampSerialized) DecodeMsg(dc *msgp.Reader) (err error) {
	var field []byte
	_ = field
	var zb0001 uint32
	zb0001, err = dc.ReadMapHeader()
	if err != nil {
		err = msgp.WrapError(err)
		return
	}
	for zb0001 > 0 {
		zb0001--
		field, err = dc.ReadMapKeyPtr()
		if err != nil {
			err = msgp.WrapError(err)
			return
		}
		switch msgp.UnsafeString(field) {
		case "vs":
			z.ValueSerialized, err = dc.ReadBytes(z.ValueSerialized)
			if err != nil {
				err = msgp.WrapError(err, "ValueSerialized")
				return
			}
		case "ts":
			z.Timestamp, err = dc.ReadInt64()
			if err != nil {
				err = msgp.WrapError(err, "Timestamp")
				return
			}
		case "injT":
			z.InjectToStream, err = dc.ReadInt64()
			if err != nil {
				err = msgp.WrapError(err, "InjectToStream")
				return
			}
		default:
			err = dc.Skip()
			if err != nil {
				err = msgp.WrapError(err)
				return
			}
		}
	}
	return
}

// EncodeMsg implements msgp.Encodable
func (z *ValueTimestampSerialized) EncodeMsg(en *msgp.Writer) (err error) {
	// omitempty: check for empty values
	zb0001Len := uint32(3)
	var zb0001Mask uint8 /* 3 bits */
	if z.ValueSerialized == nil {
		zb0001Len--
		zb0001Mask |= 0x1
	}
	if z.Timestamp == 0 {
		zb0001Len--
		zb0001Mask |= 0x2
	}
	// variable map header, size zb0001Len
	err = en.Append(0x80 | uint8(zb0001Len))
	if err != nil {
		return
	}
	if zb0001Len == 0 {
		return
	}
	if (zb0001Mask & 0x1) == 0 { // if not empty
		// write "vs"
		err = en.Append(0xa2, 0x76, 0x73)
		if err != nil {
			return
		}
		err = en.WriteBytes(z.ValueSerialized)
		if err != nil {
			err = msgp.WrapError(err, "ValueSerialized")
			return
		}
	}
	if (zb0001Mask & 0x2) == 0 { // if not empty
		// write "ts"
		err = en.Append(0xa2, 0x74, 0x73)
		if err != nil {
			return
		}
		err = en.WriteInt64(z.Timestamp)
		if err != nil {
			err = msgp.WrapError(err, "Timestamp")
			return
		}
	}
	// write "injT"
	err = en.Append(0xa4, 0x69, 0x6e, 0x6a, 0x54)
	if err != nil {
		return
	}
	err = en.WriteInt64(z.InjectToStream)
	if err != nil {
		err = msgp.WrapError(err, "InjectToStream")
		return
	}
	return
}

// MarshalMsg implements msgp.Marshaler
func (z *ValueTimestampSerialized) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	// omitempty: check for empty values
	zb0001Len := uint32(3)
	var zb0001Mask uint8 /* 3 bits */
	if z.ValueSerialized == nil {
		zb0001Len--
		zb0001Mask |= 0x1
	}
	if z.Timestamp == 0 {
		zb0001Len--
		zb0001Mask |= 0x2
	}
	// variable map header, size zb0001Len
	o = append(o, 0x80|uint8(zb0001Len))
	if zb0001Len == 0 {
		return
	}
	if (zb0001Mask & 0x1) == 0 { // if not empty
		// string "vs"
		o = append(o, 0xa2, 0x76, 0x73)
		o = msgp.AppendBytes(o, z.ValueSerialized)
	}
	if (zb0001Mask & 0x2) == 0 { // if not empty
		// string "ts"
		o = append(o, 0xa2, 0x74, 0x73)
		o = msgp.AppendInt64(o, z.Timestamp)
	}
	// string "injT"
	o = append(o, 0xa4, 0x69, 0x6e, 0x6a, 0x54)
	o = msgp.AppendInt64(o, z.InjectToStream)
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *ValueTimestampSerialized) UnmarshalMsg(bts []byte) (o []byte, err error) {
	var field []byte
	_ = field
	var zb0001 uint32
	zb0001, bts, err = msgp.ReadMapHeaderBytes(bts)
	if err != nil {
		err = msgp.WrapError(err)
		return
	}
	for zb0001 > 0 {
		zb0001--
		field, bts, err = msgp.ReadMapKeyZC(bts)
		if err != nil {
			err = msgp.WrapError(err)
			return
		}
		switch msgp.UnsafeString(field) {
		case "vs":
			z.ValueSerialized, bts, err = msgp.ReadBytesBytes(bts, z.ValueSerialized)
			if err != nil {
				err = msgp.WrapError(err, "ValueSerialized")
				return
			}
		case "ts":
			z.Timestamp, bts, err = msgp.ReadInt64Bytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "Timestamp")
				return
			}
		case "injT":
			z.InjectToStream, bts, err = msgp.ReadInt64Bytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "InjectToStream")
				return
			}
		default:
			bts, err = msgp.Skip(bts)
			if err != nil {
				err = msgp.WrapError(err)
				return
			}
		}
	}
	o = bts
	return
}

// Msgsize returns an upper bound estimate of the number of bytes occupied by the serialized message
func (z *ValueTimestampSerialized) Msgsize() (s int) {
	s = 1 + 3 + msgp.BytesPrefixSize + len(z.ValueSerialized) + 3 + msgp.Int64Size + 5 + msgp.Int64Size
	return
}
