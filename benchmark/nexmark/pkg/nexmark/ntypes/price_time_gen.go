package ntypes

// Code generated by github.com/tinylib/msgp DO NOT EDIT.

import (
	"github.com/tinylib/msgp/msgp"
)

// DecodeMsg implements msgp.Decodable
func (z *PriceTime) DecodeMsg(dc *msgp.Reader) (err error) {
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
		case "price":
			z.Price, err = dc.ReadUint64()
			if err != nil {
				err = msgp.WrapError(err, "Price")
				return
			}
		case "dateTime":
			z.DateTime, err = dc.ReadInt64()
			if err != nil {
				err = msgp.WrapError(err, "DateTime")
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
func (z PriceTime) EncodeMsg(en *msgp.Writer) (err error) {
	// map header, size 2
	// write "price"
	err = en.Append(0x82, 0xa5, 0x70, 0x72, 0x69, 0x63, 0x65)
	if err != nil {
		return
	}
	err = en.WriteUint64(z.Price)
	if err != nil {
		err = msgp.WrapError(err, "Price")
		return
	}
	// write "dateTime"
	err = en.Append(0xa8, 0x64, 0x61, 0x74, 0x65, 0x54, 0x69, 0x6d, 0x65)
	if err != nil {
		return
	}
	err = en.WriteInt64(z.DateTime)
	if err != nil {
		err = msgp.WrapError(err, "DateTime")
		return
	}
	return
}

// MarshalMsg implements msgp.Marshaler
func (z PriceTime) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	// map header, size 2
	// string "price"
	o = append(o, 0x82, 0xa5, 0x70, 0x72, 0x69, 0x63, 0x65)
	o = msgp.AppendUint64(o, z.Price)
	// string "dateTime"
	o = append(o, 0xa8, 0x64, 0x61, 0x74, 0x65, 0x54, 0x69, 0x6d, 0x65)
	o = msgp.AppendInt64(o, z.DateTime)
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *PriceTime) UnmarshalMsg(bts []byte) (o []byte, err error) {
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
		case "price":
			z.Price, bts, err = msgp.ReadUint64Bytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "Price")
				return
			}
		case "dateTime":
			z.DateTime, bts, err = msgp.ReadInt64Bytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "DateTime")
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
func (z PriceTime) Msgsize() (s int) {
	s = 1 + 6 + msgp.Uint64Size + 9 + msgp.Int64Size
	return
}