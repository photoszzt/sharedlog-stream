package commtypes

// Code generated by github.com/tinylib/msgp DO NOT EDIT.

import (
	"github.com/tinylib/msgp/msgp"
)

// DecodeMsg implements msgp.Decodable
func (z *OffsetMarker) DecodeMsg(dc *msgp.Reader) (err error) {
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
		case "offset":
			var zb0002 uint32
			zb0002, err = dc.ReadMapHeader()
			if err != nil {
				err = msgp.WrapError(err, "ConSeqNums")
				return
			}
			if z.ConSeqNums == nil {
				z.ConSeqNums = make(map[string]uint64, zb0002)
			} else if len(z.ConSeqNums) > 0 {
				for key := range z.ConSeqNums {
					delete(z.ConSeqNums, key)
				}
			}
			for zb0002 > 0 {
				zb0002--
				var za0001 string
				var za0002 uint64
				za0001, err = dc.ReadString()
				if err != nil {
					err = msgp.WrapError(err, "ConSeqNums")
					return
				}
				za0002, err = dc.ReadUint64()
				if err != nil {
					err = msgp.WrapError(err, "ConSeqNums", za0001)
					return
				}
				z.ConSeqNums[za0001] = za0002
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
func (z *OffsetMarker) EncodeMsg(en *msgp.Writer) (err error) {
	// map header, size 1
	// write "offset"
	err = en.Append(0x81, 0xa6, 0x6f, 0x66, 0x66, 0x73, 0x65, 0x74)
	if err != nil {
		return
	}
	err = en.WriteMapHeader(uint32(len(z.ConSeqNums)))
	if err != nil {
		err = msgp.WrapError(err, "ConSeqNums")
		return
	}
	for za0001, za0002 := range z.ConSeqNums {
		err = en.WriteString(za0001)
		if err != nil {
			err = msgp.WrapError(err, "ConSeqNums")
			return
		}
		err = en.WriteUint64(za0002)
		if err != nil {
			err = msgp.WrapError(err, "ConSeqNums", za0001)
			return
		}
	}
	return
}

// MarshalMsg implements msgp.Marshaler
func (z *OffsetMarker) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	// map header, size 1
	// string "offset"
	o = append(o, 0x81, 0xa6, 0x6f, 0x66, 0x66, 0x73, 0x65, 0x74)
	o = msgp.AppendMapHeader(o, uint32(len(z.ConSeqNums)))
	for za0001, za0002 := range z.ConSeqNums {
		o = msgp.AppendString(o, za0001)
		o = msgp.AppendUint64(o, za0002)
	}
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *OffsetMarker) UnmarshalMsg(bts []byte) (o []byte, err error) {
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
		case "offset":
			var zb0002 uint32
			zb0002, bts, err = msgp.ReadMapHeaderBytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "ConSeqNums")
				return
			}
			if z.ConSeqNums == nil {
				z.ConSeqNums = make(map[string]uint64, zb0002)
			} else if len(z.ConSeqNums) > 0 {
				for key := range z.ConSeqNums {
					delete(z.ConSeqNums, key)
				}
			}
			for zb0002 > 0 {
				var za0001 string
				var za0002 uint64
				zb0002--
				za0001, bts, err = msgp.ReadStringBytes(bts)
				if err != nil {
					err = msgp.WrapError(err, "ConSeqNums")
					return
				}
				za0002, bts, err = msgp.ReadUint64Bytes(bts)
				if err != nil {
					err = msgp.WrapError(err, "ConSeqNums", za0001)
					return
				}
				z.ConSeqNums[za0001] = za0002
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
func (z *OffsetMarker) Msgsize() (s int) {
	s = 1 + 7 + msgp.MapHeaderSize
	if z.ConSeqNums != nil {
		for za0001, za0002 := range z.ConSeqNums {
			_ = za0002
			s += msgp.StringPrefixSize + len(za0001) + msgp.Uint64Size
		}
	}
	return
}
