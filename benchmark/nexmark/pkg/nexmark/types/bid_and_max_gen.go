package types

// Code generated by github.com/tinylib/msgp DO NOT EDIT.

import (
	"github.com/tinylib/msgp/msgp"
)

// DecodeMsg implements msgp.Decodable
func (z *BidAndMax) DecodeMsg(dc *msgp.Reader) (err error) {
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
		case "auction":
			z.Auction, err = dc.ReadUint64()
			if err != nil {
				err = msgp.WrapError(err, "Auction")
				return
			}
		case "bidder":
			z.Bidder, err = dc.ReadUint64()
			if err != nil {
				err = msgp.WrapError(err, "Bidder")
				return
			}
		case "dateTime":
			z.DateTime, err = dc.ReadInt64()
			if err != nil {
				err = msgp.WrapError(err, "DateTime")
				return
			}
		case "maxDateTime":
			z.MaxDateTime, err = dc.ReadInt64()
			if err != nil {
				err = msgp.WrapError(err, "MaxDateTime")
				return
			}
		case "extra":
			z.Extra, err = dc.ReadString()
			if err != nil {
				err = msgp.WrapError(err, "Extra")
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
func (z *BidAndMax) EncodeMsg(en *msgp.Writer) (err error) {
	// map header, size 6
	// write "price"
	err = en.Append(0x86, 0xa5, 0x70, 0x72, 0x69, 0x63, 0x65)
	if err != nil {
		return
	}
	err = en.WriteUint64(z.Price)
	if err != nil {
		err = msgp.WrapError(err, "Price")
		return
	}
	// write "auction"
	err = en.Append(0xa7, 0x61, 0x75, 0x63, 0x74, 0x69, 0x6f, 0x6e)
	if err != nil {
		return
	}
	err = en.WriteUint64(z.Auction)
	if err != nil {
		err = msgp.WrapError(err, "Auction")
		return
	}
	// write "bidder"
	err = en.Append(0xa6, 0x62, 0x69, 0x64, 0x64, 0x65, 0x72)
	if err != nil {
		return
	}
	err = en.WriteUint64(z.Bidder)
	if err != nil {
		err = msgp.WrapError(err, "Bidder")
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
	// write "maxDateTime"
	err = en.Append(0xab, 0x6d, 0x61, 0x78, 0x44, 0x61, 0x74, 0x65, 0x54, 0x69, 0x6d, 0x65)
	if err != nil {
		return
	}
	err = en.WriteInt64(z.MaxDateTime)
	if err != nil {
		err = msgp.WrapError(err, "MaxDateTime")
		return
	}
	// write "extra"
	err = en.Append(0xa5, 0x65, 0x78, 0x74, 0x72, 0x61)
	if err != nil {
		return
	}
	err = en.WriteString(z.Extra)
	if err != nil {
		err = msgp.WrapError(err, "Extra")
		return
	}
	return
}

// MarshalMsg implements msgp.Marshaler
func (z *BidAndMax) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	// map header, size 6
	// string "price"
	o = append(o, 0x86, 0xa5, 0x70, 0x72, 0x69, 0x63, 0x65)
	o = msgp.AppendUint64(o, z.Price)
	// string "auction"
	o = append(o, 0xa7, 0x61, 0x75, 0x63, 0x74, 0x69, 0x6f, 0x6e)
	o = msgp.AppendUint64(o, z.Auction)
	// string "bidder"
	o = append(o, 0xa6, 0x62, 0x69, 0x64, 0x64, 0x65, 0x72)
	o = msgp.AppendUint64(o, z.Bidder)
	// string "dateTime"
	o = append(o, 0xa8, 0x64, 0x61, 0x74, 0x65, 0x54, 0x69, 0x6d, 0x65)
	o = msgp.AppendInt64(o, z.DateTime)
	// string "maxDateTime"
	o = append(o, 0xab, 0x6d, 0x61, 0x78, 0x44, 0x61, 0x74, 0x65, 0x54, 0x69, 0x6d, 0x65)
	o = msgp.AppendInt64(o, z.MaxDateTime)
	// string "extra"
	o = append(o, 0xa5, 0x65, 0x78, 0x74, 0x72, 0x61)
	o = msgp.AppendString(o, z.Extra)
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *BidAndMax) UnmarshalMsg(bts []byte) (o []byte, err error) {
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
		case "auction":
			z.Auction, bts, err = msgp.ReadUint64Bytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "Auction")
				return
			}
		case "bidder":
			z.Bidder, bts, err = msgp.ReadUint64Bytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "Bidder")
				return
			}
		case "dateTime":
			z.DateTime, bts, err = msgp.ReadInt64Bytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "DateTime")
				return
			}
		case "maxDateTime":
			z.MaxDateTime, bts, err = msgp.ReadInt64Bytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "MaxDateTime")
				return
			}
		case "extra":
			z.Extra, bts, err = msgp.ReadStringBytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "Extra")
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
func (z *BidAndMax) Msgsize() (s int) {
	s = 1 + 6 + msgp.Uint64Size + 8 + msgp.Uint64Size + 7 + msgp.Uint64Size + 9 + msgp.Int64Size + 12 + msgp.Int64Size + 6 + msgp.StringPrefixSize + len(z.Extra)
	return
}