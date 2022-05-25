package types

// Code generated by github.com/tinylib/msgp DO NOT EDIT.

import (
	"github.com/tinylib/msgp/msgp"
)

// DecodeMsg implements msgp.Decodable
func (z *AuctionBid) DecodeMsg(dc *msgp.Reader) (err error) {
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
		case "bidDateTime":
			z.BidDateTime, err = dc.ReadInt64()
			if err != nil {
				err = msgp.WrapError(err, "BidDateTime")
				return
			}
		case "aucDateTime":
			z.AucDateTime, err = dc.ReadInt64()
			if err != nil {
				err = msgp.WrapError(err, "AucDateTime")
				return
			}
		case "aucExpires":
			z.AucExpires, err = dc.ReadInt64()
			if err != nil {
				err = msgp.WrapError(err, "AucExpires")
				return
			}
		case "bidPrice":
			z.BidPrice, err = dc.ReadUint64()
			if err != nil {
				err = msgp.WrapError(err, "BidPrice")
				return
			}
		case "aucCategory":
			z.AucCategory, err = dc.ReadUint64()
			if err != nil {
				err = msgp.WrapError(err, "AucCategory")
				return
			}
		case "bInjT":
			err = z.BaseInjTime.DecodeMsg(dc)
			if err != nil {
				err = msgp.WrapError(err, "BaseInjTime")
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
func (z *AuctionBid) EncodeMsg(en *msgp.Writer) (err error) {
	// map header, size 6
	// write "bidDateTime"
	err = en.Append(0x86, 0xab, 0x62, 0x69, 0x64, 0x44, 0x61, 0x74, 0x65, 0x54, 0x69, 0x6d, 0x65)
	if err != nil {
		return
	}
	err = en.WriteInt64(z.BidDateTime)
	if err != nil {
		err = msgp.WrapError(err, "BidDateTime")
		return
	}
	// write "aucDateTime"
	err = en.Append(0xab, 0x61, 0x75, 0x63, 0x44, 0x61, 0x74, 0x65, 0x54, 0x69, 0x6d, 0x65)
	if err != nil {
		return
	}
	err = en.WriteInt64(z.AucDateTime)
	if err != nil {
		err = msgp.WrapError(err, "AucDateTime")
		return
	}
	// write "aucExpires"
	err = en.Append(0xaa, 0x61, 0x75, 0x63, 0x45, 0x78, 0x70, 0x69, 0x72, 0x65, 0x73)
	if err != nil {
		return
	}
	err = en.WriteInt64(z.AucExpires)
	if err != nil {
		err = msgp.WrapError(err, "AucExpires")
		return
	}
	// write "bidPrice"
	err = en.Append(0xa8, 0x62, 0x69, 0x64, 0x50, 0x72, 0x69, 0x63, 0x65)
	if err != nil {
		return
	}
	err = en.WriteUint64(z.BidPrice)
	if err != nil {
		err = msgp.WrapError(err, "BidPrice")
		return
	}
	// write "aucCategory"
	err = en.Append(0xab, 0x61, 0x75, 0x63, 0x43, 0x61, 0x74, 0x65, 0x67, 0x6f, 0x72, 0x79)
	if err != nil {
		return
	}
	err = en.WriteUint64(z.AucCategory)
	if err != nil {
		err = msgp.WrapError(err, "AucCategory")
		return
	}
	// write "bInjT"
	err = en.Append(0xa5, 0x62, 0x49, 0x6e, 0x6a, 0x54)
	if err != nil {
		return
	}
	err = z.BaseInjTime.EncodeMsg(en)
	if err != nil {
		err = msgp.WrapError(err, "BaseInjTime")
		return
	}
	return
}

// MarshalMsg implements msgp.Marshaler
func (z *AuctionBid) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	// map header, size 6
	// string "bidDateTime"
	o = append(o, 0x86, 0xab, 0x62, 0x69, 0x64, 0x44, 0x61, 0x74, 0x65, 0x54, 0x69, 0x6d, 0x65)
	o = msgp.AppendInt64(o, z.BidDateTime)
	// string "aucDateTime"
	o = append(o, 0xab, 0x61, 0x75, 0x63, 0x44, 0x61, 0x74, 0x65, 0x54, 0x69, 0x6d, 0x65)
	o = msgp.AppendInt64(o, z.AucDateTime)
	// string "aucExpires"
	o = append(o, 0xaa, 0x61, 0x75, 0x63, 0x45, 0x78, 0x70, 0x69, 0x72, 0x65, 0x73)
	o = msgp.AppendInt64(o, z.AucExpires)
	// string "bidPrice"
	o = append(o, 0xa8, 0x62, 0x69, 0x64, 0x50, 0x72, 0x69, 0x63, 0x65)
	o = msgp.AppendUint64(o, z.BidPrice)
	// string "aucCategory"
	o = append(o, 0xab, 0x61, 0x75, 0x63, 0x43, 0x61, 0x74, 0x65, 0x67, 0x6f, 0x72, 0x79)
	o = msgp.AppendUint64(o, z.AucCategory)
	// string "bInjT"
	o = append(o, 0xa5, 0x62, 0x49, 0x6e, 0x6a, 0x54)
	o, err = z.BaseInjTime.MarshalMsg(o)
	if err != nil {
		err = msgp.WrapError(err, "BaseInjTime")
		return
	}
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *AuctionBid) UnmarshalMsg(bts []byte) (o []byte, err error) {
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
		case "bidDateTime":
			z.BidDateTime, bts, err = msgp.ReadInt64Bytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "BidDateTime")
				return
			}
		case "aucDateTime":
			z.AucDateTime, bts, err = msgp.ReadInt64Bytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "AucDateTime")
				return
			}
		case "aucExpires":
			z.AucExpires, bts, err = msgp.ReadInt64Bytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "AucExpires")
				return
			}
		case "bidPrice":
			z.BidPrice, bts, err = msgp.ReadUint64Bytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "BidPrice")
				return
			}
		case "aucCategory":
			z.AucCategory, bts, err = msgp.ReadUint64Bytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "AucCategory")
				return
			}
		case "bInjT":
			bts, err = z.BaseInjTime.UnmarshalMsg(bts)
			if err != nil {
				err = msgp.WrapError(err, "BaseInjTime")
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
func (z *AuctionBid) Msgsize() (s int) {
	s = 1 + 12 + msgp.Int64Size + 12 + msgp.Int64Size + 11 + msgp.Int64Size + 9 + msgp.Uint64Size + 12 + msgp.Uint64Size + 6 + z.BaseInjTime.Msgsize()
	return
}
