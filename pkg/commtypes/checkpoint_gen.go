package commtypes

// Code generated by github.com/tinylib/msgp DO NOT EDIT.

import (
	"github.com/tinylib/msgp/msgp"
)

// DecodeMsg implements msgp.Decodable
func (z *Checkpoint) DecodeMsg(dc *msgp.Reader) (err error) {
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
		case "kvarr":
			var zb0002 uint32
			zb0002, err = dc.ReadArrayHeader()
			if err != nil {
				err = msgp.WrapError(err, "Kvarr")
				return
			}
			if cap(z.Kvarr) >= int(zb0002) {
				z.Kvarr = (z.Kvarr)[:zb0002]
			} else {
				z.Kvarr = make([][]byte, zb0002)
			}
			for za0001 := range z.Kvarr {
				z.Kvarr[za0001], err = dc.ReadBytes(z.Kvarr[za0001])
				if err != nil {
					err = msgp.WrapError(err, "Kvarr", za0001)
					return
				}
			}
		case "up":
			var zb0003 uint32
			zb0003, err = dc.ReadArrayHeader()
			if err != nil {
				err = msgp.WrapError(err, "Unprocessed")
				return
			}
			if cap(z.Unprocessed) >= int(zb0003) {
				z.Unprocessed = (z.Unprocessed)[:zb0003]
			} else {
				z.Unprocessed = make([][]uint64, zb0003)
			}
			for za0002 := range z.Unprocessed {
				var zb0004 uint32
				zb0004, err = dc.ReadArrayHeader()
				if err != nil {
					err = msgp.WrapError(err, "Unprocessed", za0002)
					return
				}
				if cap(z.Unprocessed[za0002]) >= int(zb0004) {
					z.Unprocessed[za0002] = (z.Unprocessed[za0002])[:zb0004]
				} else {
					z.Unprocessed[za0002] = make([]uint64, zb0004)
				}
				for za0003 := range z.Unprocessed[za0002] {
					z.Unprocessed[za0002][za0003], err = dc.ReadUint64()
					if err != nil {
						err = msgp.WrapError(err, "Unprocessed", za0002, za0003)
						return
					}
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
func (z *Checkpoint) EncodeMsg(en *msgp.Writer) (err error) {
	// omitempty: check for empty values
	zb0001Len := uint32(2)
	var zb0001Mask uint8 /* 2 bits */
	_ = zb0001Mask
	if z.Kvarr == nil {
		zb0001Len--
		zb0001Mask |= 0x1
	}
	if z.Unprocessed == nil {
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
		// write "kvarr"
		err = en.Append(0xa5, 0x6b, 0x76, 0x61, 0x72, 0x72)
		if err != nil {
			return
		}
		err = en.WriteArrayHeader(uint32(len(z.Kvarr)))
		if err != nil {
			err = msgp.WrapError(err, "Kvarr")
			return
		}
		for za0001 := range z.Kvarr {
			err = en.WriteBytes(z.Kvarr[za0001])
			if err != nil {
				err = msgp.WrapError(err, "Kvarr", za0001)
				return
			}
		}
	}
	if (zb0001Mask & 0x2) == 0 { // if not empty
		// write "up"
		err = en.Append(0xa2, 0x75, 0x70)
		if err != nil {
			return
		}
		err = en.WriteArrayHeader(uint32(len(z.Unprocessed)))
		if err != nil {
			err = msgp.WrapError(err, "Unprocessed")
			return
		}
		for za0002 := range z.Unprocessed {
			err = en.WriteArrayHeader(uint32(len(z.Unprocessed[za0002])))
			if err != nil {
				err = msgp.WrapError(err, "Unprocessed", za0002)
				return
			}
			for za0003 := range z.Unprocessed[za0002] {
				err = en.WriteUint64(z.Unprocessed[za0002][za0003])
				if err != nil {
					err = msgp.WrapError(err, "Unprocessed", za0002, za0003)
					return
				}
			}
		}
	}
	return
}

// MarshalMsg implements msgp.Marshaler
func (z *Checkpoint) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	// omitempty: check for empty values
	zb0001Len := uint32(2)
	var zb0001Mask uint8 /* 2 bits */
	_ = zb0001Mask
	if z.Kvarr == nil {
		zb0001Len--
		zb0001Mask |= 0x1
	}
	if z.Unprocessed == nil {
		zb0001Len--
		zb0001Mask |= 0x2
	}
	// variable map header, size zb0001Len
	o = append(o, 0x80|uint8(zb0001Len))
	if zb0001Len == 0 {
		return
	}
	if (zb0001Mask & 0x1) == 0 { // if not empty
		// string "kvarr"
		o = append(o, 0xa5, 0x6b, 0x76, 0x61, 0x72, 0x72)
		o = msgp.AppendArrayHeader(o, uint32(len(z.Kvarr)))
		for za0001 := range z.Kvarr {
			o = msgp.AppendBytes(o, z.Kvarr[za0001])
		}
	}
	if (zb0001Mask & 0x2) == 0 { // if not empty
		// string "up"
		o = append(o, 0xa2, 0x75, 0x70)
		o = msgp.AppendArrayHeader(o, uint32(len(z.Unprocessed)))
		for za0002 := range z.Unprocessed {
			o = msgp.AppendArrayHeader(o, uint32(len(z.Unprocessed[za0002])))
			for za0003 := range z.Unprocessed[za0002] {
				o = msgp.AppendUint64(o, z.Unprocessed[za0002][za0003])
			}
		}
	}
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *Checkpoint) UnmarshalMsg(bts []byte) (o []byte, err error) {
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
		case "kvarr":
			var zb0002 uint32
			zb0002, bts, err = msgp.ReadArrayHeaderBytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "Kvarr")
				return
			}
			if cap(z.Kvarr) >= int(zb0002) {
				z.Kvarr = (z.Kvarr)[:zb0002]
			} else {
				z.Kvarr = make([][]byte, zb0002)
			}
			for za0001 := range z.Kvarr {
				z.Kvarr[za0001], bts, err = msgp.ReadBytesBytes(bts, z.Kvarr[za0001])
				if err != nil {
					err = msgp.WrapError(err, "Kvarr", za0001)
					return
				}
			}
		case "up":
			var zb0003 uint32
			zb0003, bts, err = msgp.ReadArrayHeaderBytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "Unprocessed")
				return
			}
			if cap(z.Unprocessed) >= int(zb0003) {
				z.Unprocessed = (z.Unprocessed)[:zb0003]
			} else {
				z.Unprocessed = make([][]uint64, zb0003)
			}
			for za0002 := range z.Unprocessed {
				var zb0004 uint32
				zb0004, bts, err = msgp.ReadArrayHeaderBytes(bts)
				if err != nil {
					err = msgp.WrapError(err, "Unprocessed", za0002)
					return
				}
				if cap(z.Unprocessed[za0002]) >= int(zb0004) {
					z.Unprocessed[za0002] = (z.Unprocessed[za0002])[:zb0004]
				} else {
					z.Unprocessed[za0002] = make([]uint64, zb0004)
				}
				for za0003 := range z.Unprocessed[za0002] {
					z.Unprocessed[za0002][za0003], bts, err = msgp.ReadUint64Bytes(bts)
					if err != nil {
						err = msgp.WrapError(err, "Unprocessed", za0002, za0003)
						return
					}
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
func (z *Checkpoint) Msgsize() (s int) {
	s = 1 + 6 + msgp.ArrayHeaderSize
	for za0001 := range z.Kvarr {
		s += msgp.BytesPrefixSize + len(z.Kvarr[za0001])
	}
	s += 3 + msgp.ArrayHeaderSize
	for za0002 := range z.Unprocessed {
		s += msgp.ArrayHeaderSize + (len(z.Unprocessed[za0002]) * (msgp.Uint64Size))
	}
	return
}

// DecodeMsg implements msgp.Decodable
func (z *CheckpointPtrJSONSerdeG) DecodeMsg(dc *msgp.Reader) (err error) {
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
func (z CheckpointPtrJSONSerdeG) EncodeMsg(en *msgp.Writer) (err error) {
	// map header, size 0
	err = en.Append(0x80)
	if err != nil {
		return
	}
	return
}

// MarshalMsg implements msgp.Marshaler
func (z CheckpointPtrJSONSerdeG) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	// map header, size 0
	o = append(o, 0x80)
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *CheckpointPtrJSONSerdeG) UnmarshalMsg(bts []byte) (o []byte, err error) {
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
func (z CheckpointPtrJSONSerdeG) Msgsize() (s int) {
	s = 1
	return
}
