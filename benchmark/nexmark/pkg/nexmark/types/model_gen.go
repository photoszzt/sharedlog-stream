package types

// Code generated by github.com/tinylib/msgp DO NOT EDIT.

import (
	"github.com/tinylib/msgp/msgp"
)

// DecodeMsg implements msgp.Decodable
func (z *Auction) DecodeMsg(dc *msgp.Reader) (err error) {
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
		case "id":
			z.ID, err = dc.ReadUint64()
			if err != nil {
				err = msgp.WrapError(err, "ID")
				return
			}
		case "itemName":
			z.ItemName, err = dc.ReadString()
			if err != nil {
				err = msgp.WrapError(err, "ItemName")
				return
			}
		case "description":
			z.Description, err = dc.ReadString()
			if err != nil {
				err = msgp.WrapError(err, "Description")
				return
			}
		case "initialBid":
			z.InitialBid, err = dc.ReadUint64()
			if err != nil {
				err = msgp.WrapError(err, "InitialBid")
				return
			}
		case "reserve":
			z.Reserve, err = dc.ReadUint64()
			if err != nil {
				err = msgp.WrapError(err, "Reserve")
				return
			}
		case "dateTime":
			z.DateTime, err = dc.ReadInt64()
			if err != nil {
				err = msgp.WrapError(err, "DateTime")
				return
			}
		case "expires":
			z.Expires, err = dc.ReadInt64()
			if err != nil {
				err = msgp.WrapError(err, "Expires")
				return
			}
		case "seller":
			z.Seller, err = dc.ReadUint64()
			if err != nil {
				err = msgp.WrapError(err, "Seller")
				return
			}
		case "category":
			z.Category, err = dc.ReadUint64()
			if err != nil {
				err = msgp.WrapError(err, "Category")
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
func (z *Auction) EncodeMsg(en *msgp.Writer) (err error) {
	// map header, size 10
	// write "id"
	err = en.Append(0x8a, 0xa2, 0x69, 0x64)
	if err != nil {
		return
	}
	err = en.WriteUint64(z.ID)
	if err != nil {
		err = msgp.WrapError(err, "ID")
		return
	}
	// write "itemName"
	err = en.Append(0xa8, 0x69, 0x74, 0x65, 0x6d, 0x4e, 0x61, 0x6d, 0x65)
	if err != nil {
		return
	}
	err = en.WriteString(z.ItemName)
	if err != nil {
		err = msgp.WrapError(err, "ItemName")
		return
	}
	// write "description"
	err = en.Append(0xab, 0x64, 0x65, 0x73, 0x63, 0x72, 0x69, 0x70, 0x74, 0x69, 0x6f, 0x6e)
	if err != nil {
		return
	}
	err = en.WriteString(z.Description)
	if err != nil {
		err = msgp.WrapError(err, "Description")
		return
	}
	// write "initialBid"
	err = en.Append(0xaa, 0x69, 0x6e, 0x69, 0x74, 0x69, 0x61, 0x6c, 0x42, 0x69, 0x64)
	if err != nil {
		return
	}
	err = en.WriteUint64(z.InitialBid)
	if err != nil {
		err = msgp.WrapError(err, "InitialBid")
		return
	}
	// write "reserve"
	err = en.Append(0xa7, 0x72, 0x65, 0x73, 0x65, 0x72, 0x76, 0x65)
	if err != nil {
		return
	}
	err = en.WriteUint64(z.Reserve)
	if err != nil {
		err = msgp.WrapError(err, "Reserve")
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
	// write "expires"
	err = en.Append(0xa7, 0x65, 0x78, 0x70, 0x69, 0x72, 0x65, 0x73)
	if err != nil {
		return
	}
	err = en.WriteInt64(z.Expires)
	if err != nil {
		err = msgp.WrapError(err, "Expires")
		return
	}
	// write "seller"
	err = en.Append(0xa6, 0x73, 0x65, 0x6c, 0x6c, 0x65, 0x72)
	if err != nil {
		return
	}
	err = en.WriteUint64(z.Seller)
	if err != nil {
		err = msgp.WrapError(err, "Seller")
		return
	}
	// write "category"
	err = en.Append(0xa8, 0x63, 0x61, 0x74, 0x65, 0x67, 0x6f, 0x72, 0x79)
	if err != nil {
		return
	}
	err = en.WriteUint64(z.Category)
	if err != nil {
		err = msgp.WrapError(err, "Category")
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
func (z *Auction) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	// map header, size 10
	// string "id"
	o = append(o, 0x8a, 0xa2, 0x69, 0x64)
	o = msgp.AppendUint64(o, z.ID)
	// string "itemName"
	o = append(o, 0xa8, 0x69, 0x74, 0x65, 0x6d, 0x4e, 0x61, 0x6d, 0x65)
	o = msgp.AppendString(o, z.ItemName)
	// string "description"
	o = append(o, 0xab, 0x64, 0x65, 0x73, 0x63, 0x72, 0x69, 0x70, 0x74, 0x69, 0x6f, 0x6e)
	o = msgp.AppendString(o, z.Description)
	// string "initialBid"
	o = append(o, 0xaa, 0x69, 0x6e, 0x69, 0x74, 0x69, 0x61, 0x6c, 0x42, 0x69, 0x64)
	o = msgp.AppendUint64(o, z.InitialBid)
	// string "reserve"
	o = append(o, 0xa7, 0x72, 0x65, 0x73, 0x65, 0x72, 0x76, 0x65)
	o = msgp.AppendUint64(o, z.Reserve)
	// string "dateTime"
	o = append(o, 0xa8, 0x64, 0x61, 0x74, 0x65, 0x54, 0x69, 0x6d, 0x65)
	o = msgp.AppendInt64(o, z.DateTime)
	// string "expires"
	o = append(o, 0xa7, 0x65, 0x78, 0x70, 0x69, 0x72, 0x65, 0x73)
	o = msgp.AppendInt64(o, z.Expires)
	// string "seller"
	o = append(o, 0xa6, 0x73, 0x65, 0x6c, 0x6c, 0x65, 0x72)
	o = msgp.AppendUint64(o, z.Seller)
	// string "category"
	o = append(o, 0xa8, 0x63, 0x61, 0x74, 0x65, 0x67, 0x6f, 0x72, 0x79)
	o = msgp.AppendUint64(o, z.Category)
	// string "extra"
	o = append(o, 0xa5, 0x65, 0x78, 0x74, 0x72, 0x61)
	o = msgp.AppendString(o, z.Extra)
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *Auction) UnmarshalMsg(bts []byte) (o []byte, err error) {
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
		case "id":
			z.ID, bts, err = msgp.ReadUint64Bytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "ID")
				return
			}
		case "itemName":
			z.ItemName, bts, err = msgp.ReadStringBytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "ItemName")
				return
			}
		case "description":
			z.Description, bts, err = msgp.ReadStringBytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "Description")
				return
			}
		case "initialBid":
			z.InitialBid, bts, err = msgp.ReadUint64Bytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "InitialBid")
				return
			}
		case "reserve":
			z.Reserve, bts, err = msgp.ReadUint64Bytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "Reserve")
				return
			}
		case "dateTime":
			z.DateTime, bts, err = msgp.ReadInt64Bytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "DateTime")
				return
			}
		case "expires":
			z.Expires, bts, err = msgp.ReadInt64Bytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "Expires")
				return
			}
		case "seller":
			z.Seller, bts, err = msgp.ReadUint64Bytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "Seller")
				return
			}
		case "category":
			z.Category, bts, err = msgp.ReadUint64Bytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "Category")
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
func (z *Auction) Msgsize() (s int) {
	s = 1 + 3 + msgp.Uint64Size + 9 + msgp.StringPrefixSize + len(z.ItemName) + 12 + msgp.StringPrefixSize + len(z.Description) + 11 + msgp.Uint64Size + 8 + msgp.Uint64Size + 9 + msgp.Int64Size + 8 + msgp.Int64Size + 7 + msgp.Uint64Size + 9 + msgp.Uint64Size + 6 + msgp.StringPrefixSize + len(z.Extra)
	return
}

// DecodeMsg implements msgp.Decodable
func (z *Bid) DecodeMsg(dc *msgp.Reader) (err error) {
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
		case "price":
			z.Price, err = dc.ReadUint64()
			if err != nil {
				err = msgp.WrapError(err, "Price")
				return
			}
		case "channel":
			z.Channel, err = dc.ReadString()
			if err != nil {
				err = msgp.WrapError(err, "Channel")
				return
			}
		case "url":
			z.Url, err = dc.ReadString()
			if err != nil {
				err = msgp.WrapError(err, "Url")
				return
			}
		case "dateTime":
			z.DateTime, err = dc.ReadInt64()
			if err != nil {
				err = msgp.WrapError(err, "DateTime")
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
func (z *Bid) EncodeMsg(en *msgp.Writer) (err error) {
	// map header, size 7
	// write "auction"
	err = en.Append(0x87, 0xa7, 0x61, 0x75, 0x63, 0x74, 0x69, 0x6f, 0x6e)
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
	// write "price"
	err = en.Append(0xa5, 0x70, 0x72, 0x69, 0x63, 0x65)
	if err != nil {
		return
	}
	err = en.WriteUint64(z.Price)
	if err != nil {
		err = msgp.WrapError(err, "Price")
		return
	}
	// write "channel"
	err = en.Append(0xa7, 0x63, 0x68, 0x61, 0x6e, 0x6e, 0x65, 0x6c)
	if err != nil {
		return
	}
	err = en.WriteString(z.Channel)
	if err != nil {
		err = msgp.WrapError(err, "Channel")
		return
	}
	// write "url"
	err = en.Append(0xa3, 0x75, 0x72, 0x6c)
	if err != nil {
		return
	}
	err = en.WriteString(z.Url)
	if err != nil {
		err = msgp.WrapError(err, "Url")
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
func (z *Bid) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	// map header, size 7
	// string "auction"
	o = append(o, 0x87, 0xa7, 0x61, 0x75, 0x63, 0x74, 0x69, 0x6f, 0x6e)
	o = msgp.AppendUint64(o, z.Auction)
	// string "bidder"
	o = append(o, 0xa6, 0x62, 0x69, 0x64, 0x64, 0x65, 0x72)
	o = msgp.AppendUint64(o, z.Bidder)
	// string "price"
	o = append(o, 0xa5, 0x70, 0x72, 0x69, 0x63, 0x65)
	o = msgp.AppendUint64(o, z.Price)
	// string "channel"
	o = append(o, 0xa7, 0x63, 0x68, 0x61, 0x6e, 0x6e, 0x65, 0x6c)
	o = msgp.AppendString(o, z.Channel)
	// string "url"
	o = append(o, 0xa3, 0x75, 0x72, 0x6c)
	o = msgp.AppendString(o, z.Url)
	// string "dateTime"
	o = append(o, 0xa8, 0x64, 0x61, 0x74, 0x65, 0x54, 0x69, 0x6d, 0x65)
	o = msgp.AppendInt64(o, z.DateTime)
	// string "extra"
	o = append(o, 0xa5, 0x65, 0x78, 0x74, 0x72, 0x61)
	o = msgp.AppendString(o, z.Extra)
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *Bid) UnmarshalMsg(bts []byte) (o []byte, err error) {
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
		case "price":
			z.Price, bts, err = msgp.ReadUint64Bytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "Price")
				return
			}
		case "channel":
			z.Channel, bts, err = msgp.ReadStringBytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "Channel")
				return
			}
		case "url":
			z.Url, bts, err = msgp.ReadStringBytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "Url")
				return
			}
		case "dateTime":
			z.DateTime, bts, err = msgp.ReadInt64Bytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "DateTime")
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
func (z *Bid) Msgsize() (s int) {
	s = 1 + 8 + msgp.Uint64Size + 7 + msgp.Uint64Size + 6 + msgp.Uint64Size + 8 + msgp.StringPrefixSize + len(z.Channel) + 4 + msgp.StringPrefixSize + len(z.Url) + 9 + msgp.Int64Size + 6 + msgp.StringPrefixSize + len(z.Extra)
	return
}

// DecodeMsg implements msgp.Decodable
func (z *EType) DecodeMsg(dc *msgp.Reader) (err error) {
	{
		var zb0001 uint8
		zb0001, err = dc.ReadUint8()
		if err != nil {
			err = msgp.WrapError(err)
			return
		}
		(*z) = EType(zb0001)
	}
	return
}

// EncodeMsg implements msgp.Encodable
func (z EType) EncodeMsg(en *msgp.Writer) (err error) {
	err = en.WriteUint8(uint8(z))
	if err != nil {
		err = msgp.WrapError(err)
		return
	}
	return
}

// MarshalMsg implements msgp.Marshaler
func (z EType) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	o = msgp.AppendUint8(o, uint8(z))
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *EType) UnmarshalMsg(bts []byte) (o []byte, err error) {
	{
		var zb0001 uint8
		zb0001, bts, err = msgp.ReadUint8Bytes(bts)
		if err != nil {
			err = msgp.WrapError(err)
			return
		}
		(*z) = EType(zb0001)
	}
	o = bts
	return
}

// Msgsize returns an upper bound estimate of the number of bytes occupied by the serialized message
func (z EType) Msgsize() (s int) {
	s = msgp.Uint8Size
	return
}

// DecodeMsg implements msgp.Decodable
func (z *Event) DecodeMsg(dc *msgp.Reader) (err error) {
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
		case "Etype":
			{
				var zb0002 uint8
				zb0002, err = dc.ReadUint8()
				if err != nil {
					err = msgp.WrapError(err, "Etype")
					return
				}
				z.Etype = EType(zb0002)
			}
		case "NewPerson":
			if dc.IsNil() {
				err = dc.ReadNil()
				if err != nil {
					err = msgp.WrapError(err, "NewPerson")
					return
				}
				z.NewPerson = nil
			} else {
				if z.NewPerson == nil {
					z.NewPerson = new(Person)
				}
				err = z.NewPerson.DecodeMsg(dc)
				if err != nil {
					err = msgp.WrapError(err, "NewPerson")
					return
				}
			}
		case "NewAuction":
			if dc.IsNil() {
				err = dc.ReadNil()
				if err != nil {
					err = msgp.WrapError(err, "NewAuction")
					return
				}
				z.NewAuction = nil
			} else {
				if z.NewAuction == nil {
					z.NewAuction = new(Auction)
				}
				err = z.NewAuction.DecodeMsg(dc)
				if err != nil {
					err = msgp.WrapError(err, "NewAuction")
					return
				}
			}
		case "Bid":
			if dc.IsNil() {
				err = dc.ReadNil()
				if err != nil {
					err = msgp.WrapError(err, "Bid")
					return
				}
				z.Bid = nil
			} else {
				if z.Bid == nil {
					z.Bid = new(Bid)
				}
				err = z.Bid.DecodeMsg(dc)
				if err != nil {
					err = msgp.WrapError(err, "Bid")
					return
				}
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
func (z *Event) EncodeMsg(en *msgp.Writer) (err error) {
	// map header, size 4
	// write "Etype"
	err = en.Append(0x84, 0xa5, 0x45, 0x74, 0x79, 0x70, 0x65)
	if err != nil {
		return
	}
	err = en.WriteUint8(uint8(z.Etype))
	if err != nil {
		err = msgp.WrapError(err, "Etype")
		return
	}
	// write "NewPerson"
	err = en.Append(0xa9, 0x4e, 0x65, 0x77, 0x50, 0x65, 0x72, 0x73, 0x6f, 0x6e)
	if err != nil {
		return
	}
	if z.NewPerson == nil {
		err = en.WriteNil()
		if err != nil {
			return
		}
	} else {
		err = z.NewPerson.EncodeMsg(en)
		if err != nil {
			err = msgp.WrapError(err, "NewPerson")
			return
		}
	}
	// write "NewAuction"
	err = en.Append(0xaa, 0x4e, 0x65, 0x77, 0x41, 0x75, 0x63, 0x74, 0x69, 0x6f, 0x6e)
	if err != nil {
		return
	}
	if z.NewAuction == nil {
		err = en.WriteNil()
		if err != nil {
			return
		}
	} else {
		err = z.NewAuction.EncodeMsg(en)
		if err != nil {
			err = msgp.WrapError(err, "NewAuction")
			return
		}
	}
	// write "Bid"
	err = en.Append(0xa3, 0x42, 0x69, 0x64)
	if err != nil {
		return
	}
	if z.Bid == nil {
		err = en.WriteNil()
		if err != nil {
			return
		}
	} else {
		err = z.Bid.EncodeMsg(en)
		if err != nil {
			err = msgp.WrapError(err, "Bid")
			return
		}
	}
	return
}

// MarshalMsg implements msgp.Marshaler
func (z *Event) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	// map header, size 4
	// string "Etype"
	o = append(o, 0x84, 0xa5, 0x45, 0x74, 0x79, 0x70, 0x65)
	o = msgp.AppendUint8(o, uint8(z.Etype))
	// string "NewPerson"
	o = append(o, 0xa9, 0x4e, 0x65, 0x77, 0x50, 0x65, 0x72, 0x73, 0x6f, 0x6e)
	if z.NewPerson == nil {
		o = msgp.AppendNil(o)
	} else {
		o, err = z.NewPerson.MarshalMsg(o)
		if err != nil {
			err = msgp.WrapError(err, "NewPerson")
			return
		}
	}
	// string "NewAuction"
	o = append(o, 0xaa, 0x4e, 0x65, 0x77, 0x41, 0x75, 0x63, 0x74, 0x69, 0x6f, 0x6e)
	if z.NewAuction == nil {
		o = msgp.AppendNil(o)
	} else {
		o, err = z.NewAuction.MarshalMsg(o)
		if err != nil {
			err = msgp.WrapError(err, "NewAuction")
			return
		}
	}
	// string "Bid"
	o = append(o, 0xa3, 0x42, 0x69, 0x64)
	if z.Bid == nil {
		o = msgp.AppendNil(o)
	} else {
		o, err = z.Bid.MarshalMsg(o)
		if err != nil {
			err = msgp.WrapError(err, "Bid")
			return
		}
	}
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *Event) UnmarshalMsg(bts []byte) (o []byte, err error) {
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
		case "Etype":
			{
				var zb0002 uint8
				zb0002, bts, err = msgp.ReadUint8Bytes(bts)
				if err != nil {
					err = msgp.WrapError(err, "Etype")
					return
				}
				z.Etype = EType(zb0002)
			}
		case "NewPerson":
			if msgp.IsNil(bts) {
				bts, err = msgp.ReadNilBytes(bts)
				if err != nil {
					return
				}
				z.NewPerson = nil
			} else {
				if z.NewPerson == nil {
					z.NewPerson = new(Person)
				}
				bts, err = z.NewPerson.UnmarshalMsg(bts)
				if err != nil {
					err = msgp.WrapError(err, "NewPerson")
					return
				}
			}
		case "NewAuction":
			if msgp.IsNil(bts) {
				bts, err = msgp.ReadNilBytes(bts)
				if err != nil {
					return
				}
				z.NewAuction = nil
			} else {
				if z.NewAuction == nil {
					z.NewAuction = new(Auction)
				}
				bts, err = z.NewAuction.UnmarshalMsg(bts)
				if err != nil {
					err = msgp.WrapError(err, "NewAuction")
					return
				}
			}
		case "Bid":
			if msgp.IsNil(bts) {
				bts, err = msgp.ReadNilBytes(bts)
				if err != nil {
					return
				}
				z.Bid = nil
			} else {
				if z.Bid == nil {
					z.Bid = new(Bid)
				}
				bts, err = z.Bid.UnmarshalMsg(bts)
				if err != nil {
					err = msgp.WrapError(err, "Bid")
					return
				}
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
func (z *Event) Msgsize() (s int) {
	s = 1 + 6 + msgp.Uint8Size + 10
	if z.NewPerson == nil {
		s += msgp.NilSize
	} else {
		s += z.NewPerson.Msgsize()
	}
	s += 11
	if z.NewAuction == nil {
		s += msgp.NilSize
	} else {
		s += z.NewAuction.Msgsize()
	}
	s += 4
	if z.Bid == nil {
		s += msgp.NilSize
	} else {
		s += z.Bid.Msgsize()
	}
	return
}

// DecodeMsg implements msgp.Decodable
func (z *EventJSONSerde) DecodeMsg(dc *msgp.Reader) (err error) {
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
func (z EventJSONSerde) EncodeMsg(en *msgp.Writer) (err error) {
	// map header, size 0
	err = en.Append(0x80)
	if err != nil {
		return
	}
	return
}

// MarshalMsg implements msgp.Marshaler
func (z EventJSONSerde) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	// map header, size 0
	o = append(o, 0x80)
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *EventJSONSerde) UnmarshalMsg(bts []byte) (o []byte, err error) {
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
func (z EventJSONSerde) Msgsize() (s int) {
	s = 1
	return
}

// DecodeMsg implements msgp.Decodable
func (z *EventMsgpSerde) DecodeMsg(dc *msgp.Reader) (err error) {
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
func (z EventMsgpSerde) EncodeMsg(en *msgp.Writer) (err error) {
	// map header, size 0
	err = en.Append(0x80)
	if err != nil {
		return
	}
	return
}

// MarshalMsg implements msgp.Marshaler
func (z EventMsgpSerde) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	// map header, size 0
	o = append(o, 0x80)
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *EventMsgpSerde) UnmarshalMsg(bts []byte) (o []byte, err error) {
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
func (z EventMsgpSerde) Msgsize() (s int) {
	s = 1
	return
}

// DecodeMsg implements msgp.Decodable
func (z *NameCityStateId) DecodeMsg(dc *msgp.Reader) (err error) {
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
		case "Name":
			z.Name, err = dc.ReadString()
			if err != nil {
				err = msgp.WrapError(err, "Name")
				return
			}
		case "City":
			z.City, err = dc.ReadString()
			if err != nil {
				err = msgp.WrapError(err, "City")
				return
			}
		case "State":
			z.State, err = dc.ReadString()
			if err != nil {
				err = msgp.WrapError(err, "State")
				return
			}
		case "ID":
			z.ID, err = dc.ReadUint64()
			if err != nil {
				err = msgp.WrapError(err, "ID")
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
func (z *NameCityStateId) EncodeMsg(en *msgp.Writer) (err error) {
	// map header, size 4
	// write "Name"
	err = en.Append(0x84, 0xa4, 0x4e, 0x61, 0x6d, 0x65)
	if err != nil {
		return
	}
	err = en.WriteString(z.Name)
	if err != nil {
		err = msgp.WrapError(err, "Name")
		return
	}
	// write "City"
	err = en.Append(0xa4, 0x43, 0x69, 0x74, 0x79)
	if err != nil {
		return
	}
	err = en.WriteString(z.City)
	if err != nil {
		err = msgp.WrapError(err, "City")
		return
	}
	// write "State"
	err = en.Append(0xa5, 0x53, 0x74, 0x61, 0x74, 0x65)
	if err != nil {
		return
	}
	err = en.WriteString(z.State)
	if err != nil {
		err = msgp.WrapError(err, "State")
		return
	}
	// write "ID"
	err = en.Append(0xa2, 0x49, 0x44)
	if err != nil {
		return
	}
	err = en.WriteUint64(z.ID)
	if err != nil {
		err = msgp.WrapError(err, "ID")
		return
	}
	return
}

// MarshalMsg implements msgp.Marshaler
func (z *NameCityStateId) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	// map header, size 4
	// string "Name"
	o = append(o, 0x84, 0xa4, 0x4e, 0x61, 0x6d, 0x65)
	o = msgp.AppendString(o, z.Name)
	// string "City"
	o = append(o, 0xa4, 0x43, 0x69, 0x74, 0x79)
	o = msgp.AppendString(o, z.City)
	// string "State"
	o = append(o, 0xa5, 0x53, 0x74, 0x61, 0x74, 0x65)
	o = msgp.AppendString(o, z.State)
	// string "ID"
	o = append(o, 0xa2, 0x49, 0x44)
	o = msgp.AppendUint64(o, z.ID)
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *NameCityStateId) UnmarshalMsg(bts []byte) (o []byte, err error) {
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
		case "Name":
			z.Name, bts, err = msgp.ReadStringBytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "Name")
				return
			}
		case "City":
			z.City, bts, err = msgp.ReadStringBytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "City")
				return
			}
		case "State":
			z.State, bts, err = msgp.ReadStringBytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "State")
				return
			}
		case "ID":
			z.ID, bts, err = msgp.ReadUint64Bytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "ID")
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
func (z *NameCityStateId) Msgsize() (s int) {
	s = 1 + 5 + msgp.StringPrefixSize + len(z.Name) + 5 + msgp.StringPrefixSize + len(z.City) + 6 + msgp.StringPrefixSize + len(z.State) + 3 + msgp.Uint64Size
	return
}

// DecodeMsg implements msgp.Decodable
func (z *Person) DecodeMsg(dc *msgp.Reader) (err error) {
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
		case "id":
			z.ID, err = dc.ReadUint64()
			if err != nil {
				err = msgp.WrapError(err, "ID")
				return
			}
		case "name":
			z.Name, err = dc.ReadString()
			if err != nil {
				err = msgp.WrapError(err, "Name")
				return
			}
		case "emailAddress":
			z.EmailAddress, err = dc.ReadString()
			if err != nil {
				err = msgp.WrapError(err, "EmailAddress")
				return
			}
		case "creditCard":
			z.CreditCard, err = dc.ReadString()
			if err != nil {
				err = msgp.WrapError(err, "CreditCard")
				return
			}
		case "city":
			z.City, err = dc.ReadString()
			if err != nil {
				err = msgp.WrapError(err, "City")
				return
			}
		case "state":
			z.State, err = dc.ReadString()
			if err != nil {
				err = msgp.WrapError(err, "State")
				return
			}
		case "dateTime":
			z.DateTime, err = dc.ReadInt64()
			if err != nil {
				err = msgp.WrapError(err, "DateTime")
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
func (z *Person) EncodeMsg(en *msgp.Writer) (err error) {
	// map header, size 8
	// write "id"
	err = en.Append(0x88, 0xa2, 0x69, 0x64)
	if err != nil {
		return
	}
	err = en.WriteUint64(z.ID)
	if err != nil {
		err = msgp.WrapError(err, "ID")
		return
	}
	// write "name"
	err = en.Append(0xa4, 0x6e, 0x61, 0x6d, 0x65)
	if err != nil {
		return
	}
	err = en.WriteString(z.Name)
	if err != nil {
		err = msgp.WrapError(err, "Name")
		return
	}
	// write "emailAddress"
	err = en.Append(0xac, 0x65, 0x6d, 0x61, 0x69, 0x6c, 0x41, 0x64, 0x64, 0x72, 0x65, 0x73, 0x73)
	if err != nil {
		return
	}
	err = en.WriteString(z.EmailAddress)
	if err != nil {
		err = msgp.WrapError(err, "EmailAddress")
		return
	}
	// write "creditCard"
	err = en.Append(0xaa, 0x63, 0x72, 0x65, 0x64, 0x69, 0x74, 0x43, 0x61, 0x72, 0x64)
	if err != nil {
		return
	}
	err = en.WriteString(z.CreditCard)
	if err != nil {
		err = msgp.WrapError(err, "CreditCard")
		return
	}
	// write "city"
	err = en.Append(0xa4, 0x63, 0x69, 0x74, 0x79)
	if err != nil {
		return
	}
	err = en.WriteString(z.City)
	if err != nil {
		err = msgp.WrapError(err, "City")
		return
	}
	// write "state"
	err = en.Append(0xa5, 0x73, 0x74, 0x61, 0x74, 0x65)
	if err != nil {
		return
	}
	err = en.WriteString(z.State)
	if err != nil {
		err = msgp.WrapError(err, "State")
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
func (z *Person) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	// map header, size 8
	// string "id"
	o = append(o, 0x88, 0xa2, 0x69, 0x64)
	o = msgp.AppendUint64(o, z.ID)
	// string "name"
	o = append(o, 0xa4, 0x6e, 0x61, 0x6d, 0x65)
	o = msgp.AppendString(o, z.Name)
	// string "emailAddress"
	o = append(o, 0xac, 0x65, 0x6d, 0x61, 0x69, 0x6c, 0x41, 0x64, 0x64, 0x72, 0x65, 0x73, 0x73)
	o = msgp.AppendString(o, z.EmailAddress)
	// string "creditCard"
	o = append(o, 0xaa, 0x63, 0x72, 0x65, 0x64, 0x69, 0x74, 0x43, 0x61, 0x72, 0x64)
	o = msgp.AppendString(o, z.CreditCard)
	// string "city"
	o = append(o, 0xa4, 0x63, 0x69, 0x74, 0x79)
	o = msgp.AppendString(o, z.City)
	// string "state"
	o = append(o, 0xa5, 0x73, 0x74, 0x61, 0x74, 0x65)
	o = msgp.AppendString(o, z.State)
	// string "dateTime"
	o = append(o, 0xa8, 0x64, 0x61, 0x74, 0x65, 0x54, 0x69, 0x6d, 0x65)
	o = msgp.AppendInt64(o, z.DateTime)
	// string "extra"
	o = append(o, 0xa5, 0x65, 0x78, 0x74, 0x72, 0x61)
	o = msgp.AppendString(o, z.Extra)
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *Person) UnmarshalMsg(bts []byte) (o []byte, err error) {
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
		case "id":
			z.ID, bts, err = msgp.ReadUint64Bytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "ID")
				return
			}
		case "name":
			z.Name, bts, err = msgp.ReadStringBytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "Name")
				return
			}
		case "emailAddress":
			z.EmailAddress, bts, err = msgp.ReadStringBytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "EmailAddress")
				return
			}
		case "creditCard":
			z.CreditCard, bts, err = msgp.ReadStringBytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "CreditCard")
				return
			}
		case "city":
			z.City, bts, err = msgp.ReadStringBytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "City")
				return
			}
		case "state":
			z.State, bts, err = msgp.ReadStringBytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "State")
				return
			}
		case "dateTime":
			z.DateTime, bts, err = msgp.ReadInt64Bytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "DateTime")
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
func (z *Person) Msgsize() (s int) {
	s = 1 + 3 + msgp.Uint64Size + 5 + msgp.StringPrefixSize + len(z.Name) + 13 + msgp.StringPrefixSize + len(z.EmailAddress) + 11 + msgp.StringPrefixSize + len(z.CreditCard) + 5 + msgp.StringPrefixSize + len(z.City) + 6 + msgp.StringPrefixSize + len(z.State) + 9 + msgp.Int64Size + 6 + msgp.StringPrefixSize + len(z.Extra)
	return
}
