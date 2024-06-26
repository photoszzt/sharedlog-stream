package ntypes

// Code generated by github.com/tinylib/msgp DO NOT EDIT.

import (
	"github.com/tinylib/msgp/msgp"
)

// DecodeMsg implements msgp.Decodable
func (z *StartEndTime) DecodeMsg(dc *msgp.Reader) (err error) {
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
		case "sTs":
			z.StartTimeMs, err = dc.ReadInt64()
			if err != nil {
				err = msgp.WrapError(err, "StartTimeMs")
				return
			}
		case "eTs":
			z.EndTimeMs, err = dc.ReadInt64()
			if err != nil {
				err = msgp.WrapError(err, "EndTimeMs")
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
func (z StartEndTime) EncodeMsg(en *msgp.Writer) (err error) {
	// map header, size 2
	// write "sTs"
	err = en.Append(0x82, 0xa3, 0x73, 0x54, 0x73)
	if err != nil {
		return
	}
	err = en.WriteInt64(z.StartTimeMs)
	if err != nil {
		err = msgp.WrapError(err, "StartTimeMs")
		return
	}
	// write "eTs"
	err = en.Append(0xa3, 0x65, 0x54, 0x73)
	if err != nil {
		return
	}
	err = en.WriteInt64(z.EndTimeMs)
	if err != nil {
		err = msgp.WrapError(err, "EndTimeMs")
		return
	}
	return
}

// MarshalMsg implements msgp.Marshaler
func (z StartEndTime) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	// map header, size 2
	// string "sTs"
	o = append(o, 0x82, 0xa3, 0x73, 0x54, 0x73)
	o = msgp.AppendInt64(o, z.StartTimeMs)
	// string "eTs"
	o = append(o, 0xa3, 0x65, 0x54, 0x73)
	o = msgp.AppendInt64(o, z.EndTimeMs)
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *StartEndTime) UnmarshalMsg(bts []byte) (o []byte, err error) {
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
		case "sTs":
			z.StartTimeMs, bts, err = msgp.ReadInt64Bytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "StartTimeMs")
				return
			}
		case "eTs":
			z.EndTimeMs, bts, err = msgp.ReadInt64Bytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "EndTimeMs")
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
func (z StartEndTime) Msgsize() (s int) {
	s = 1 + 4 + msgp.Int64Size + 4 + msgp.Int64Size
	return
}
