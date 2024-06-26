package ntypes

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
		case "bTs":
			z.BidTs, err = dc.ReadInt64()
			if err != nil {
				err = msgp.WrapError(err, "BidTs")
				return
			}
		case "wStartMs":
			z.WStartMs, err = dc.ReadInt64()
			if err != nil {
				err = msgp.WrapError(err, "WStartMs")
				return
			}
		case "wEndMs":
			z.WEndMs, err = dc.ReadInt64()
			if err != nil {
				err = msgp.WrapError(err, "WEndMs")
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
	// write "bTs"
	err = en.Append(0xa3, 0x62, 0x54, 0x73)
	if err != nil {
		return
	}
	err = en.WriteInt64(z.BidTs)
	if err != nil {
		err = msgp.WrapError(err, "BidTs")
		return
	}
	// write "wStartMs"
	err = en.Append(0xa8, 0x77, 0x53, 0x74, 0x61, 0x72, 0x74, 0x4d, 0x73)
	if err != nil {
		return
	}
	err = en.WriteInt64(z.WStartMs)
	if err != nil {
		err = msgp.WrapError(err, "WStartMs")
		return
	}
	// write "wEndMs"
	err = en.Append(0xa6, 0x77, 0x45, 0x6e, 0x64, 0x4d, 0x73)
	if err != nil {
		return
	}
	err = en.WriteInt64(z.WEndMs)
	if err != nil {
		err = msgp.WrapError(err, "WEndMs")
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
	// string "bTs"
	o = append(o, 0xa3, 0x62, 0x54, 0x73)
	o = msgp.AppendInt64(o, z.BidTs)
	// string "wStartMs"
	o = append(o, 0xa8, 0x77, 0x53, 0x74, 0x61, 0x72, 0x74, 0x4d, 0x73)
	o = msgp.AppendInt64(o, z.WStartMs)
	// string "wEndMs"
	o = append(o, 0xa6, 0x77, 0x45, 0x6e, 0x64, 0x4d, 0x73)
	o = msgp.AppendInt64(o, z.WEndMs)
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
		case "bTs":
			z.BidTs, bts, err = msgp.ReadInt64Bytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "BidTs")
				return
			}
		case "wStartMs":
			z.WStartMs, bts, err = msgp.ReadInt64Bytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "WStartMs")
				return
			}
		case "wEndMs":
			z.WEndMs, bts, err = msgp.ReadInt64Bytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "WEndMs")
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
	s = 1 + 6 + msgp.Uint64Size + 8 + msgp.Uint64Size + 7 + msgp.Uint64Size + 4 + msgp.Int64Size + 9 + msgp.Int64Size + 7 + msgp.Int64Size
	return
}
