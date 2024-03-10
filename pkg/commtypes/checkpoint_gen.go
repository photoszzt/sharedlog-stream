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
				err = msgp.WrapError(err, "KvArr")
				return
			}
			if cap(z.KvArr) >= int(zb0002) {
				z.KvArr = (z.KvArr)[:zb0002]
			} else {
				z.KvArr = make([][]byte, zb0002)
			}
			for za0001 := range z.KvArr {
				z.KvArr[za0001], err = dc.ReadBytes(z.KvArr[za0001])
				if err != nil {
					err = msgp.WrapError(err, "KvArr", za0001)
					return
				}
			}
		case "chkptm":
			var zb0003 uint32
			zb0003, err = dc.ReadArrayHeader()
			if err != nil {
				err = msgp.WrapError(err, "ChkptMeta")
				return
			}
			if cap(z.ChkptMeta) >= int(zb0003) {
				z.ChkptMeta = (z.ChkptMeta)[:zb0003]
			} else {
				z.ChkptMeta = make([]ChkptMetaData, zb0003)
			}
			for za0002 := range z.ChkptMeta {
				var zb0004 uint32
				zb0004, err = dc.ReadMapHeader()
				if err != nil {
					err = msgp.WrapError(err, "ChkptMeta", za0002)
					return
				}
				for zb0004 > 0 {
					zb0004--
					field, err = dc.ReadMapKeyPtr()
					if err != nil {
						err = msgp.WrapError(err, "ChkptMeta", za0002)
						return
					}
					switch msgp.UnsafeString(field) {
					case "up":
						var zb0005 uint32
						zb0005, err = dc.ReadArrayHeader()
						if err != nil {
							err = msgp.WrapError(err, "ChkptMeta", za0002, "Unprocessed")
							return
						}
						if cap(z.ChkptMeta[za0002].Unprocessed) >= int(zb0005) {
							z.ChkptMeta[za0002].Unprocessed = (z.ChkptMeta[za0002].Unprocessed)[:zb0005]
						} else {
							z.ChkptMeta[za0002].Unprocessed = make([]uint64, zb0005)
						}
						for za0003 := range z.ChkptMeta[za0002].Unprocessed {
							z.ChkptMeta[za0002].Unprocessed[za0003], err = dc.ReadUint64()
							if err != nil {
								err = msgp.WrapError(err, "ChkptMeta", za0002, "Unprocessed", za0003)
								return
							}
						}
					case "lcpm":
						z.ChkptMeta[za0002].LastChkptMarker, err = dc.ReadUint64()
						if err != nil {
							err = msgp.WrapError(err, "ChkptMeta", za0002, "LastChkptMarker")
							return
						}
					default:
						err = dc.Skip()
						if err != nil {
							err = msgp.WrapError(err, "ChkptMeta", za0002)
							return
						}
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
	if z.KvArr == nil {
		zb0001Len--
		zb0001Mask |= 0x1
	}
	if z.ChkptMeta == nil {
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
		err = en.WriteArrayHeader(uint32(len(z.KvArr)))
		if err != nil {
			err = msgp.WrapError(err, "KvArr")
			return
		}
		for za0001 := range z.KvArr {
			err = en.WriteBytes(z.KvArr[za0001])
			if err != nil {
				err = msgp.WrapError(err, "KvArr", za0001)
				return
			}
		}
	}
	if (zb0001Mask & 0x2) == 0 { // if not empty
		// write "chkptm"
		err = en.Append(0xa6, 0x63, 0x68, 0x6b, 0x70, 0x74, 0x6d)
		if err != nil {
			return
		}
		err = en.WriteArrayHeader(uint32(len(z.ChkptMeta)))
		if err != nil {
			err = msgp.WrapError(err, "ChkptMeta")
			return
		}
		for za0002 := range z.ChkptMeta {
			// omitempty: check for empty values
			zb0002Len := uint32(2)
			var zb0002Mask uint8 /* 2 bits */
			_ = zb0002Mask
			if z.ChkptMeta[za0002].Unprocessed == nil {
				zb0002Len--
				zb0002Mask |= 0x1
			}
			if z.ChkptMeta[za0002].LastChkptMarker == 0 {
				zb0002Len--
				zb0002Mask |= 0x2
			}
			// variable map header, size zb0002Len
			err = en.Append(0x80 | uint8(zb0002Len))
			if err != nil {
				return
			}
			if (zb0002Mask & 0x1) == 0 { // if not empty
				// write "up"
				err = en.Append(0xa2, 0x75, 0x70)
				if err != nil {
					return
				}
				err = en.WriteArrayHeader(uint32(len(z.ChkptMeta[za0002].Unprocessed)))
				if err != nil {
					err = msgp.WrapError(err, "ChkptMeta", za0002, "Unprocessed")
					return
				}
				for za0003 := range z.ChkptMeta[za0002].Unprocessed {
					err = en.WriteUint64(z.ChkptMeta[za0002].Unprocessed[za0003])
					if err != nil {
						err = msgp.WrapError(err, "ChkptMeta", za0002, "Unprocessed", za0003)
						return
					}
				}
			}
			if (zb0002Mask & 0x2) == 0 { // if not empty
				// write "lcpm"
				err = en.Append(0xa4, 0x6c, 0x63, 0x70, 0x6d)
				if err != nil {
					return
				}
				err = en.WriteUint64(z.ChkptMeta[za0002].LastChkptMarker)
				if err != nil {
					err = msgp.WrapError(err, "ChkptMeta", za0002, "LastChkptMarker")
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
	if z.KvArr == nil {
		zb0001Len--
		zb0001Mask |= 0x1
	}
	if z.ChkptMeta == nil {
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
		o = msgp.AppendArrayHeader(o, uint32(len(z.KvArr)))
		for za0001 := range z.KvArr {
			o = msgp.AppendBytes(o, z.KvArr[za0001])
		}
	}
	if (zb0001Mask & 0x2) == 0 { // if not empty
		// string "chkptm"
		o = append(o, 0xa6, 0x63, 0x68, 0x6b, 0x70, 0x74, 0x6d)
		o = msgp.AppendArrayHeader(o, uint32(len(z.ChkptMeta)))
		for za0002 := range z.ChkptMeta {
			// omitempty: check for empty values
			zb0002Len := uint32(2)
			var zb0002Mask uint8 /* 2 bits */
			_ = zb0002Mask
			if z.ChkptMeta[za0002].Unprocessed == nil {
				zb0002Len--
				zb0002Mask |= 0x1
			}
			if z.ChkptMeta[za0002].LastChkptMarker == 0 {
				zb0002Len--
				zb0002Mask |= 0x2
			}
			// variable map header, size zb0002Len
			o = append(o, 0x80|uint8(zb0002Len))
			if (zb0002Mask & 0x1) == 0 { // if not empty
				// string "up"
				o = append(o, 0xa2, 0x75, 0x70)
				o = msgp.AppendArrayHeader(o, uint32(len(z.ChkptMeta[za0002].Unprocessed)))
				for za0003 := range z.ChkptMeta[za0002].Unprocessed {
					o = msgp.AppendUint64(o, z.ChkptMeta[za0002].Unprocessed[za0003])
				}
			}
			if (zb0002Mask & 0x2) == 0 { // if not empty
				// string "lcpm"
				o = append(o, 0xa4, 0x6c, 0x63, 0x70, 0x6d)
				o = msgp.AppendUint64(o, z.ChkptMeta[za0002].LastChkptMarker)
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
				err = msgp.WrapError(err, "KvArr")
				return
			}
			if cap(z.KvArr) >= int(zb0002) {
				z.KvArr = (z.KvArr)[:zb0002]
			} else {
				z.KvArr = make([][]byte, zb0002)
			}
			for za0001 := range z.KvArr {
				z.KvArr[za0001], bts, err = msgp.ReadBytesBytes(bts, z.KvArr[za0001])
				if err != nil {
					err = msgp.WrapError(err, "KvArr", za0001)
					return
				}
			}
		case "chkptm":
			var zb0003 uint32
			zb0003, bts, err = msgp.ReadArrayHeaderBytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "ChkptMeta")
				return
			}
			if cap(z.ChkptMeta) >= int(zb0003) {
				z.ChkptMeta = (z.ChkptMeta)[:zb0003]
			} else {
				z.ChkptMeta = make([]ChkptMetaData, zb0003)
			}
			for za0002 := range z.ChkptMeta {
				var zb0004 uint32
				zb0004, bts, err = msgp.ReadMapHeaderBytes(bts)
				if err != nil {
					err = msgp.WrapError(err, "ChkptMeta", za0002)
					return
				}
				for zb0004 > 0 {
					zb0004--
					field, bts, err = msgp.ReadMapKeyZC(bts)
					if err != nil {
						err = msgp.WrapError(err, "ChkptMeta", za0002)
						return
					}
					switch msgp.UnsafeString(field) {
					case "up":
						var zb0005 uint32
						zb0005, bts, err = msgp.ReadArrayHeaderBytes(bts)
						if err != nil {
							err = msgp.WrapError(err, "ChkptMeta", za0002, "Unprocessed")
							return
						}
						if cap(z.ChkptMeta[za0002].Unprocessed) >= int(zb0005) {
							z.ChkptMeta[za0002].Unprocessed = (z.ChkptMeta[za0002].Unprocessed)[:zb0005]
						} else {
							z.ChkptMeta[za0002].Unprocessed = make([]uint64, zb0005)
						}
						for za0003 := range z.ChkptMeta[za0002].Unprocessed {
							z.ChkptMeta[za0002].Unprocessed[za0003], bts, err = msgp.ReadUint64Bytes(bts)
							if err != nil {
								err = msgp.WrapError(err, "ChkptMeta", za0002, "Unprocessed", za0003)
								return
							}
						}
					case "lcpm":
						z.ChkptMeta[za0002].LastChkptMarker, bts, err = msgp.ReadUint64Bytes(bts)
						if err != nil {
							err = msgp.WrapError(err, "ChkptMeta", za0002, "LastChkptMarker")
							return
						}
					default:
						bts, err = msgp.Skip(bts)
						if err != nil {
							err = msgp.WrapError(err, "ChkptMeta", za0002)
							return
						}
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
	for za0001 := range z.KvArr {
		s += msgp.BytesPrefixSize + len(z.KvArr[za0001])
	}
	s += 7 + msgp.ArrayHeaderSize
	for za0002 := range z.ChkptMeta {
		s += 1 + 3 + msgp.ArrayHeaderSize + (len(z.ChkptMeta[za0002].Unprocessed) * (msgp.Uint64Size)) + 5 + msgp.Uint64Size
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

// DecodeMsg implements msgp.Decodable
func (z *ChkptMetaData) DecodeMsg(dc *msgp.Reader) (err error) {
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
		case "up":
			var zb0002 uint32
			zb0002, err = dc.ReadArrayHeader()
			if err != nil {
				err = msgp.WrapError(err, "Unprocessed")
				return
			}
			if cap(z.Unprocessed) >= int(zb0002) {
				z.Unprocessed = (z.Unprocessed)[:zb0002]
			} else {
				z.Unprocessed = make([]uint64, zb0002)
			}
			for za0001 := range z.Unprocessed {
				z.Unprocessed[za0001], err = dc.ReadUint64()
				if err != nil {
					err = msgp.WrapError(err, "Unprocessed", za0001)
					return
				}
			}
		case "lcpm":
			z.LastChkptMarker, err = dc.ReadUint64()
			if err != nil {
				err = msgp.WrapError(err, "LastChkptMarker")
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
func (z *ChkptMetaData) EncodeMsg(en *msgp.Writer) (err error) {
	// omitempty: check for empty values
	zb0001Len := uint32(2)
	var zb0001Mask uint8 /* 2 bits */
	_ = zb0001Mask
	if z.Unprocessed == nil {
		zb0001Len--
		zb0001Mask |= 0x1
	}
	if z.LastChkptMarker == 0 {
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
		for za0001 := range z.Unprocessed {
			err = en.WriteUint64(z.Unprocessed[za0001])
			if err != nil {
				err = msgp.WrapError(err, "Unprocessed", za0001)
				return
			}
		}
	}
	if (zb0001Mask & 0x2) == 0 { // if not empty
		// write "lcpm"
		err = en.Append(0xa4, 0x6c, 0x63, 0x70, 0x6d)
		if err != nil {
			return
		}
		err = en.WriteUint64(z.LastChkptMarker)
		if err != nil {
			err = msgp.WrapError(err, "LastChkptMarker")
			return
		}
	}
	return
}

// MarshalMsg implements msgp.Marshaler
func (z *ChkptMetaData) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	// omitempty: check for empty values
	zb0001Len := uint32(2)
	var zb0001Mask uint8 /* 2 bits */
	_ = zb0001Mask
	if z.Unprocessed == nil {
		zb0001Len--
		zb0001Mask |= 0x1
	}
	if z.LastChkptMarker == 0 {
		zb0001Len--
		zb0001Mask |= 0x2
	}
	// variable map header, size zb0001Len
	o = append(o, 0x80|uint8(zb0001Len))
	if zb0001Len == 0 {
		return
	}
	if (zb0001Mask & 0x1) == 0 { // if not empty
		// string "up"
		o = append(o, 0xa2, 0x75, 0x70)
		o = msgp.AppendArrayHeader(o, uint32(len(z.Unprocessed)))
		for za0001 := range z.Unprocessed {
			o = msgp.AppendUint64(o, z.Unprocessed[za0001])
		}
	}
	if (zb0001Mask & 0x2) == 0 { // if not empty
		// string "lcpm"
		o = append(o, 0xa4, 0x6c, 0x63, 0x70, 0x6d)
		o = msgp.AppendUint64(o, z.LastChkptMarker)
	}
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *ChkptMetaData) UnmarshalMsg(bts []byte) (o []byte, err error) {
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
		case "up":
			var zb0002 uint32
			zb0002, bts, err = msgp.ReadArrayHeaderBytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "Unprocessed")
				return
			}
			if cap(z.Unprocessed) >= int(zb0002) {
				z.Unprocessed = (z.Unprocessed)[:zb0002]
			} else {
				z.Unprocessed = make([]uint64, zb0002)
			}
			for za0001 := range z.Unprocessed {
				z.Unprocessed[za0001], bts, err = msgp.ReadUint64Bytes(bts)
				if err != nil {
					err = msgp.WrapError(err, "Unprocessed", za0001)
					return
				}
			}
		case "lcpm":
			z.LastChkptMarker, bts, err = msgp.ReadUint64Bytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "LastChkptMarker")
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
func (z *ChkptMetaData) Msgsize() (s int) {
	s = 1 + 3 + msgp.ArrayHeaderSize + (len(z.Unprocessed) * (msgp.Uint64Size)) + 5 + msgp.Uint64Size
	return
}