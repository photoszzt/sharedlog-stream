package commtypes

// Code generated by github.com/tinylib/msgp DO NOT EDIT.

import (
	"github.com/tinylib/msgp/msgp"
)

// DecodeMsg implements msgp.Decodable
func (z *BaseWindow) DecodeMsg(dc *msgp.Reader) (err error) {
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
		case "startTs":
			z.StartTs, err = dc.ReadInt64()
			if err != nil {
				err = msgp.WrapError(err, "StartTs")
				return
			}
		case "endTs":
			z.EndTs, err = dc.ReadInt64()
			if err != nil {
				err = msgp.WrapError(err, "EndTs")
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
func (z BaseWindow) EncodeMsg(en *msgp.Writer) (err error) {
	// map header, size 2
	// write "startTs"
	err = en.Append(0x82, 0xa7, 0x73, 0x74, 0x61, 0x72, 0x74, 0x54, 0x73)
	if err != nil {
		return
	}
	err = en.WriteInt64(z.StartTs)
	if err != nil {
		err = msgp.WrapError(err, "StartTs")
		return
	}
	// write "endTs"
	err = en.Append(0xa5, 0x65, 0x6e, 0x64, 0x54, 0x73)
	if err != nil {
		return
	}
	err = en.WriteInt64(z.EndTs)
	if err != nil {
		err = msgp.WrapError(err, "EndTs")
		return
	}
	return
}

// MarshalMsg implements msgp.Marshaler
func (z BaseWindow) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	// map header, size 2
	// string "startTs"
	o = append(o, 0x82, 0xa7, 0x73, 0x74, 0x61, 0x72, 0x74, 0x54, 0x73)
	o = msgp.AppendInt64(o, z.StartTs)
	// string "endTs"
	o = append(o, 0xa5, 0x65, 0x6e, 0x64, 0x54, 0x73)
	o = msgp.AppendInt64(o, z.EndTs)
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *BaseWindow) UnmarshalMsg(bts []byte) (o []byte, err error) {
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
		case "startTs":
			z.StartTs, bts, err = msgp.ReadInt64Bytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "StartTs")
				return
			}
		case "endTs":
			z.EndTs, bts, err = msgp.ReadInt64Bytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "EndTs")
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
func (z BaseWindow) Msgsize() (s int) {
	s = 1 + 8 + msgp.Int64Size + 6 + msgp.Int64Size
	return
}
