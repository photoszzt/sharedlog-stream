package commtypes

// Code generated by github.com/tinylib/msgp DO NOT EDIT.

import (
	"github.com/tinylib/msgp/msgp"
)

// DecodeMsg implements msgp.Decodable
func (z *MessageSerialized) DecodeMsg(dc *msgp.Reader) (err error) {
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
		case "key":
			z.KeyEnc, err = dc.ReadBytes(z.KeyEnc)
			if err != nil {
				err = msgp.WrapError(err, "KeyEnc")
				return
			}
		case "val":
			z.ValueEnc, err = dc.ReadBytes(z.ValueEnc)
			if err != nil {
				err = msgp.WrapError(err, "ValueEnc")
				return
			}
		case "injT":
			z.InjTMs, err = dc.ReadInt64()
			if err != nil {
				err = msgp.WrapError(err, "InjTMs")
				return
			}
		case "ts":
			z.TimestampMs, err = dc.ReadInt64()
			if err != nil {
				err = msgp.WrapError(err, "TimestampMs")
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
func (z *MessageSerialized) EncodeMsg(en *msgp.Writer) (err error) {
	// omitempty: check for empty values
	zb0001Len := uint32(4)
	var zb0001Mask uint8 /* 4 bits */
	if z.KeyEnc == nil {
		zb0001Len--
		zb0001Mask |= 0x1
	}
	if z.ValueEnc == nil {
		zb0001Len--
		zb0001Mask |= 0x2
	}
	if z.TimestampMs == 0 {
		zb0001Len--
		zb0001Mask |= 0x8
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
		// write "key"
		err = en.Append(0xa3, 0x6b, 0x65, 0x79)
		if err != nil {
			return
		}
		err = en.WriteBytes(z.KeyEnc)
		if err != nil {
			err = msgp.WrapError(err, "KeyEnc")
			return
		}
	}
	if (zb0001Mask & 0x2) == 0 { // if not empty
		// write "val"
		err = en.Append(0xa3, 0x76, 0x61, 0x6c)
		if err != nil {
			return
		}
		err = en.WriteBytes(z.ValueEnc)
		if err != nil {
			err = msgp.WrapError(err, "ValueEnc")
			return
		}
	}
	// write "injT"
	err = en.Append(0xa4, 0x69, 0x6e, 0x6a, 0x54)
	if err != nil {
		return
	}
	err = en.WriteInt64(z.InjTMs)
	if err != nil {
		err = msgp.WrapError(err, "InjTMs")
		return
	}
	if (zb0001Mask & 0x8) == 0 { // if not empty
		// write "ts"
		err = en.Append(0xa2, 0x74, 0x73)
		if err != nil {
			return
		}
		err = en.WriteInt64(z.TimestampMs)
		if err != nil {
			err = msgp.WrapError(err, "TimestampMs")
			return
		}
	}
	return
}

// MarshalMsg implements msgp.Marshaler
func (z *MessageSerialized) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	// omitempty: check for empty values
	zb0001Len := uint32(4)
	var zb0001Mask uint8 /* 4 bits */
	if z.KeyEnc == nil {
		zb0001Len--
		zb0001Mask |= 0x1
	}
	if z.ValueEnc == nil {
		zb0001Len--
		zb0001Mask |= 0x2
	}
	if z.TimestampMs == 0 {
		zb0001Len--
		zb0001Mask |= 0x8
	}
	// variable map header, size zb0001Len
	o = append(o, 0x80|uint8(zb0001Len))
	if zb0001Len == 0 {
		return
	}
	if (zb0001Mask & 0x1) == 0 { // if not empty
		// string "key"
		o = append(o, 0xa3, 0x6b, 0x65, 0x79)
		o = msgp.AppendBytes(o, z.KeyEnc)
	}
	if (zb0001Mask & 0x2) == 0 { // if not empty
		// string "val"
		o = append(o, 0xa3, 0x76, 0x61, 0x6c)
		o = msgp.AppendBytes(o, z.ValueEnc)
	}
	// string "injT"
	o = append(o, 0xa4, 0x69, 0x6e, 0x6a, 0x54)
	o = msgp.AppendInt64(o, z.InjTMs)
	if (zb0001Mask & 0x8) == 0 { // if not empty
		// string "ts"
		o = append(o, 0xa2, 0x74, 0x73)
		o = msgp.AppendInt64(o, z.TimestampMs)
	}
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *MessageSerialized) UnmarshalMsg(bts []byte) (o []byte, err error) {
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
		case "key":
			z.KeyEnc, bts, err = msgp.ReadBytesBytes(bts, z.KeyEnc)
			if err != nil {
				err = msgp.WrapError(err, "KeyEnc")
				return
			}
		case "val":
			z.ValueEnc, bts, err = msgp.ReadBytesBytes(bts, z.ValueEnc)
			if err != nil {
				err = msgp.WrapError(err, "ValueEnc")
				return
			}
		case "injT":
			z.InjTMs, bts, err = msgp.ReadInt64Bytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "InjTMs")
				return
			}
		case "ts":
			z.TimestampMs, bts, err = msgp.ReadInt64Bytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "TimestampMs")
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
func (z *MessageSerialized) Msgsize() (s int) {
	s = 1 + 4 + msgp.BytesPrefixSize + len(z.KeyEnc) + 4 + msgp.BytesPrefixSize + len(z.ValueEnc) + 5 + msgp.Int64Size + 3 + msgp.Int64Size
	return
}
