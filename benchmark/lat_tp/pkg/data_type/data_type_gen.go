package datatype

// Code generated by github.com/tinylib/msgp DO NOT EDIT.

import (
	"github.com/tinylib/msgp/msgp"
)

// DecodeMsg implements msgp.Decodable
func (z *PayloadTs) DecodeMsg(dc *msgp.Reader) (err error) {
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
		case "pl":
			z.Payload, err = dc.ReadBytes(z.Payload)
			if err != nil {
				err = msgp.WrapError(err, "Payload")
				return
			}
		case "ts":
			z.Ts, err = dc.ReadInt64()
			if err != nil {
				err = msgp.WrapError(err, "Ts")
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
func (z *PayloadTs) EncodeMsg(en *msgp.Writer) (err error) {
	// map header, size 2
	// write "pl"
	err = en.Append(0x82, 0xa2, 0x70, 0x6c)
	if err != nil {
		return
	}
	err = en.WriteBytes(z.Payload)
	if err != nil {
		err = msgp.WrapError(err, "Payload")
		return
	}
	// write "ts"
	err = en.Append(0xa2, 0x74, 0x73)
	if err != nil {
		return
	}
	err = en.WriteInt64(z.Ts)
	if err != nil {
		err = msgp.WrapError(err, "Ts")
		return
	}
	return
}

// MarshalMsg implements msgp.Marshaler
func (z *PayloadTs) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	// map header, size 2
	// string "pl"
	o = append(o, 0x82, 0xa2, 0x70, 0x6c)
	o = msgp.AppendBytes(o, z.Payload)
	// string "ts"
	o = append(o, 0xa2, 0x74, 0x73)
	o = msgp.AppendInt64(o, z.Ts)
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *PayloadTs) UnmarshalMsg(bts []byte) (o []byte, err error) {
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
		case "pl":
			z.Payload, bts, err = msgp.ReadBytesBytes(bts, z.Payload)
			if err != nil {
				err = msgp.WrapError(err, "Payload")
				return
			}
		case "ts":
			z.Ts, bts, err = msgp.ReadInt64Bytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "Ts")
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
func (z *PayloadTs) Msgsize() (s int) {
	s = 1 + 3 + msgp.BytesPrefixSize + len(z.Payload) + 3 + msgp.Int64Size
	return
}
