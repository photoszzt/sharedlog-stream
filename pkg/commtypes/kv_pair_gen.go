package commtypes

// Code generated by github.com/tinylib/msgp DO NOT EDIT.

import (
	"github.com/tinylib/msgp/msgp"
)

// DecodeMsg implements msgp.Decodable
func (z *KeyValueSerrialized) DecodeMsg(dc *msgp.Reader) (err error) {
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
func (z *KeyValueSerrialized) EncodeMsg(en *msgp.Writer) (err error) {
	// omitempty: check for empty values
	zb0001Len := uint32(2)
	var zb0001Mask uint8 /* 2 bits */
	_ = zb0001Mask
	if z.KeyEnc == nil {
		zb0001Len--
		zb0001Mask |= 0x1
	}
	if z.ValueEnc == nil {
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
	return
}

// MarshalMsg implements msgp.Marshaler
func (z *KeyValueSerrialized) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	// omitempty: check for empty values
	zb0001Len := uint32(2)
	var zb0001Mask uint8 /* 2 bits */
	_ = zb0001Mask
	if z.KeyEnc == nil {
		zb0001Len--
		zb0001Mask |= 0x1
	}
	if z.ValueEnc == nil {
		zb0001Len--
		zb0001Mask |= 0x2
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
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *KeyValueSerrialized) UnmarshalMsg(bts []byte) (o []byte, err error) {
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
func (z *KeyValueSerrialized) Msgsize() (s int) {
	s = 1 + 4 + msgp.BytesPrefixSize + len(z.KeyEnc) + 4 + msgp.BytesPrefixSize + len(z.ValueEnc)
	return
}

// DecodeMsg implements msgp.Decodable
func (z *KeyValueSers) DecodeMsg(dc *msgp.Reader) (err error) {
	var zb0002 uint32
	zb0002, err = dc.ReadArrayHeader()
	if err != nil {
		err = msgp.WrapError(err)
		return
	}
	if cap((*z)) >= int(zb0002) {
		(*z) = (*z)[:zb0002]
	} else {
		(*z) = make(KeyValueSers, zb0002)
	}
	for zb0001 := range *z {
		var field []byte
		_ = field
		var zb0003 uint32
		zb0003, err = dc.ReadMapHeader()
		if err != nil {
			err = msgp.WrapError(err, zb0001)
			return
		}
		for zb0003 > 0 {
			zb0003--
			field, err = dc.ReadMapKeyPtr()
			if err != nil {
				err = msgp.WrapError(err, zb0001)
				return
			}
			switch msgp.UnsafeString(field) {
			case "key":
				(*z)[zb0001].KeyEnc, err = dc.ReadBytes((*z)[zb0001].KeyEnc)
				if err != nil {
					err = msgp.WrapError(err, zb0001, "KeyEnc")
					return
				}
			case "val":
				(*z)[zb0001].ValueEnc, err = dc.ReadBytes((*z)[zb0001].ValueEnc)
				if err != nil {
					err = msgp.WrapError(err, zb0001, "ValueEnc")
					return
				}
			default:
				err = dc.Skip()
				if err != nil {
					err = msgp.WrapError(err, zb0001)
					return
				}
			}
		}
	}
	return
}

// EncodeMsg implements msgp.Encodable
func (z KeyValueSers) EncodeMsg(en *msgp.Writer) (err error) {
	err = en.WriteArrayHeader(uint32(len(z)))
	if err != nil {
		err = msgp.WrapError(err)
		return
	}
	for zb0004 := range z {
		// omitempty: check for empty values
		zb0001Len := uint32(2)
		var zb0001Mask uint8 /* 2 bits */
		_ = zb0001Mask
		if z[zb0004].KeyEnc == nil {
			zb0001Len--
			zb0001Mask |= 0x1
		}
		if z[zb0004].ValueEnc == nil {
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
			// write "key"
			err = en.Append(0xa3, 0x6b, 0x65, 0x79)
			if err != nil {
				return
			}
			err = en.WriteBytes(z[zb0004].KeyEnc)
			if err != nil {
				err = msgp.WrapError(err, zb0004, "KeyEnc")
				return
			}
		}
		if (zb0001Mask & 0x2) == 0 { // if not empty
			// write "val"
			err = en.Append(0xa3, 0x76, 0x61, 0x6c)
			if err != nil {
				return
			}
			err = en.WriteBytes(z[zb0004].ValueEnc)
			if err != nil {
				err = msgp.WrapError(err, zb0004, "ValueEnc")
				return
			}
		}
	}
	return
}

// MarshalMsg implements msgp.Marshaler
func (z KeyValueSers) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	o = msgp.AppendArrayHeader(o, uint32(len(z)))
	for zb0004 := range z {
		// omitempty: check for empty values
		zb0001Len := uint32(2)
		var zb0001Mask uint8 /* 2 bits */
		_ = zb0001Mask
		if z[zb0004].KeyEnc == nil {
			zb0001Len--
			zb0001Mask |= 0x1
		}
		if z[zb0004].ValueEnc == nil {
			zb0001Len--
			zb0001Mask |= 0x2
		}
		// variable map header, size zb0001Len
		o = append(o, 0x80|uint8(zb0001Len))
		if zb0001Len == 0 {
			return
		}
		if (zb0001Mask & 0x1) == 0 { // if not empty
			// string "key"
			o = append(o, 0xa3, 0x6b, 0x65, 0x79)
			o = msgp.AppendBytes(o, z[zb0004].KeyEnc)
		}
		if (zb0001Mask & 0x2) == 0 { // if not empty
			// string "val"
			o = append(o, 0xa3, 0x76, 0x61, 0x6c)
			o = msgp.AppendBytes(o, z[zb0004].ValueEnc)
		}
	}
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *KeyValueSers) UnmarshalMsg(bts []byte) (o []byte, err error) {
	var zb0002 uint32
	zb0002, bts, err = msgp.ReadArrayHeaderBytes(bts)
	if err != nil {
		err = msgp.WrapError(err)
		return
	}
	if cap((*z)) >= int(zb0002) {
		(*z) = (*z)[:zb0002]
	} else {
		(*z) = make(KeyValueSers, zb0002)
	}
	for zb0001 := range *z {
		var field []byte
		_ = field
		var zb0003 uint32
		zb0003, bts, err = msgp.ReadMapHeaderBytes(bts)
		if err != nil {
			err = msgp.WrapError(err, zb0001)
			return
		}
		for zb0003 > 0 {
			zb0003--
			field, bts, err = msgp.ReadMapKeyZC(bts)
			if err != nil {
				err = msgp.WrapError(err, zb0001)
				return
			}
			switch msgp.UnsafeString(field) {
			case "key":
				(*z)[zb0001].KeyEnc, bts, err = msgp.ReadBytesBytes(bts, (*z)[zb0001].KeyEnc)
				if err != nil {
					err = msgp.WrapError(err, zb0001, "KeyEnc")
					return
				}
			case "val":
				(*z)[zb0001].ValueEnc, bts, err = msgp.ReadBytesBytes(bts, (*z)[zb0001].ValueEnc)
				if err != nil {
					err = msgp.WrapError(err, zb0001, "ValueEnc")
					return
				}
			default:
				bts, err = msgp.Skip(bts)
				if err != nil {
					err = msgp.WrapError(err, zb0001)
					return
				}
			}
		}
	}
	o = bts
	return
}

// Msgsize returns an upper bound estimate of the number of bytes occupied by the serialized message
func (z KeyValueSers) Msgsize() (s int) {
	s = msgp.ArrayHeaderSize
	for zb0004 := range z {
		s += 1 + 4 + msgp.BytesPrefixSize + len(z[zb0004].KeyEnc) + 4 + msgp.BytesPrefixSize + len(z[zb0004].ValueEnc)
	}
	return
}
