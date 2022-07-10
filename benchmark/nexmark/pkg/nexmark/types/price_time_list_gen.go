package types

// Code generated by github.com/tinylib/msgp DO NOT EDIT.

import (
	"github.com/tinylib/msgp/msgp"
)

// DecodeMsg implements msgp.Decodable
func (z *PriceTimeList) DecodeMsg(dc *msgp.Reader) (err error) {
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
		case "ptList":
			var zb0002 uint32
			zb0002, err = dc.ReadArrayHeader()
			if err != nil {
				err = msgp.WrapError(err, "PTList")
				return
			}
			if cap(z.PTList) >= int(zb0002) {
				z.PTList = (z.PTList)[:zb0002]
			} else {
				z.PTList = make(PriceTimeSlice, zb0002)
			}
			for za0001 := range z.PTList {
				err = z.PTList[za0001].DecodeMsg(dc)
				if err != nil {
					err = msgp.WrapError(err, "PTList", za0001)
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
func (z *PriceTimeList) EncodeMsg(en *msgp.Writer) (err error) {
	// map header, size 1
	// write "ptList"
	err = en.Append(0x81, 0xa6, 0x70, 0x74, 0x4c, 0x69, 0x73, 0x74)
	if err != nil {
		return
	}
	err = en.WriteArrayHeader(uint32(len(z.PTList)))
	if err != nil {
		err = msgp.WrapError(err, "PTList")
		return
	}
	for za0001 := range z.PTList {
		err = z.PTList[za0001].EncodeMsg(en)
		if err != nil {
			err = msgp.WrapError(err, "PTList", za0001)
			return
		}
	}
	return
}

// MarshalMsg implements msgp.Marshaler
func (z *PriceTimeList) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	// map header, size 1
	// string "ptList"
	o = append(o, 0x81, 0xa6, 0x70, 0x74, 0x4c, 0x69, 0x73, 0x74)
	o = msgp.AppendArrayHeader(o, uint32(len(z.PTList)))
	for za0001 := range z.PTList {
		o, err = z.PTList[za0001].MarshalMsg(o)
		if err != nil {
			err = msgp.WrapError(err, "PTList", za0001)
			return
		}
	}
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *PriceTimeList) UnmarshalMsg(bts []byte) (o []byte, err error) {
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
		case "ptList":
			var zb0002 uint32
			zb0002, bts, err = msgp.ReadArrayHeaderBytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "PTList")
				return
			}
			if cap(z.PTList) >= int(zb0002) {
				z.PTList = (z.PTList)[:zb0002]
			} else {
				z.PTList = make(PriceTimeSlice, zb0002)
			}
			for za0001 := range z.PTList {
				bts, err = z.PTList[za0001].UnmarshalMsg(bts)
				if err != nil {
					err = msgp.WrapError(err, "PTList", za0001)
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
func (z *PriceTimeList) Msgsize() (s int) {
	s = 1 + 7 + msgp.ArrayHeaderSize
	for za0001 := range z.PTList {
		s += z.PTList[za0001].Msgsize()
	}
	return
}

// DecodeMsg implements msgp.Decodable
func (z *PriceTimeSlice) DecodeMsg(dc *msgp.Reader) (err error) {
	var zb0002 uint32
	zb0002, err = dc.ReadArrayHeader()
	if err != nil {
		err = msgp.WrapError(err)
		return
	}
	if cap((*z)) >= int(zb0002) {
		(*z) = (*z)[:zb0002]
	} else {
		(*z) = make(PriceTimeSlice, zb0002)
	}
	for zb0001 := range *z {
		err = (*z)[zb0001].DecodeMsg(dc)
		if err != nil {
			err = msgp.WrapError(err, zb0001)
			return
		}
	}
	return
}

// EncodeMsg implements msgp.Encodable
func (z PriceTimeSlice) EncodeMsg(en *msgp.Writer) (err error) {
	err = en.WriteArrayHeader(uint32(len(z)))
	if err != nil {
		err = msgp.WrapError(err)
		return
	}
	for zb0003 := range z {
		err = z[zb0003].EncodeMsg(en)
		if err != nil {
			err = msgp.WrapError(err, zb0003)
			return
		}
	}
	return
}

// MarshalMsg implements msgp.Marshaler
func (z PriceTimeSlice) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	o = msgp.AppendArrayHeader(o, uint32(len(z)))
	for zb0003 := range z {
		o, err = z[zb0003].MarshalMsg(o)
		if err != nil {
			err = msgp.WrapError(err, zb0003)
			return
		}
	}
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *PriceTimeSlice) UnmarshalMsg(bts []byte) (o []byte, err error) {
	var zb0002 uint32
	zb0002, bts, err = msgp.ReadArrayHeaderBytes(bts)
	if err != nil {
		err = msgp.WrapError(err)
		return
	}
	if cap((*z)) >= int(zb0002) {
		(*z) = (*z)[:zb0002]
	} else {
		(*z) = make(PriceTimeSlice, zb0002)
	}
	for zb0001 := range *z {
		bts, err = (*z)[zb0001].UnmarshalMsg(bts)
		if err != nil {
			err = msgp.WrapError(err, zb0001)
			return
		}
	}
	o = bts
	return
}

// Msgsize returns an upper bound estimate of the number of bytes occupied by the serialized message
func (z PriceTimeSlice) Msgsize() (s int) {
	s = msgp.ArrayHeaderSize
	for zb0003 := range z {
		s += z[zb0003].Msgsize()
	}
	return
}
