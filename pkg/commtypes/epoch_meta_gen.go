package commtypes

// Code generated by github.com/tinylib/msgp DO NOT EDIT.

import (
	"github.com/tinylib/msgp/msgp"
)

// DecodeMsg implements msgp.Decodable
func (z *EpochMark) DecodeMsg(dc *msgp.Reader) (err error) {
	{
		var zb0001 uint8
		zb0001, err = dc.ReadUint8()
		if err != nil {
			err = msgp.WrapError(err)
			return
		}
		(*z) = EpochMark(zb0001)
	}
	return
}

// EncodeMsg implements msgp.Encodable
func (z EpochMark) EncodeMsg(en *msgp.Writer) (err error) {
	err = en.WriteUint8(uint8(z))
	if err != nil {
		err = msgp.WrapError(err)
		return
	}
	return
}

// MarshalMsg implements msgp.Marshaler
func (z EpochMark) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	o = msgp.AppendUint8(o, uint8(z))
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *EpochMark) UnmarshalMsg(bts []byte) (o []byte, err error) {
	{
		var zb0001 uint8
		zb0001, bts, err = msgp.ReadUint8Bytes(bts)
		if err != nil {
			err = msgp.WrapError(err)
			return
		}
		(*z) = EpochMark(zb0001)
	}
	o = bts
	return
}

// Msgsize returns an upper bound estimate of the number of bytes occupied by the serialized message
func (z EpochMark) Msgsize() (s int) {
	s = msgp.Uint8Size
	return
}

// DecodeMsg implements msgp.Decodable
func (z *EpochMarker) DecodeMsg(dc *msgp.Reader) (err error) {
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
		case "ConSeqNum":
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
		case "outRng":
			var zb0003 uint32
			zb0003, err = dc.ReadMapHeader()
			if err != nil {
				err = msgp.WrapError(err, "OutputRanges")
				return
			}
			if z.OutputRanges == nil {
				z.OutputRanges = make(map[string][]ProduceRange, zb0003)
			} else if len(z.OutputRanges) > 0 {
				for key := range z.OutputRanges {
					delete(z.OutputRanges, key)
				}
			}
			for zb0003 > 0 {
				zb0003--
				var za0003 string
				var za0004 []ProduceRange
				za0003, err = dc.ReadString()
				if err != nil {
					err = msgp.WrapError(err, "OutputRanges")
					return
				}
				var zb0004 uint32
				zb0004, err = dc.ReadArrayHeader()
				if err != nil {
					err = msgp.WrapError(err, "OutputRanges", za0003)
					return
				}
				if cap(za0004) >= int(zb0004) {
					za0004 = (za0004)[:zb0004]
				} else {
					za0004 = make([]ProduceRange, zb0004)
				}
				for za0005 := range za0004 {
					var zb0005 uint32
					zb0005, err = dc.ReadMapHeader()
					if err != nil {
						err = msgp.WrapError(err, "OutputRanges", za0003, za0005)
						return
					}
					for zb0005 > 0 {
						zb0005--
						field, err = dc.ReadMapKeyPtr()
						if err != nil {
							err = msgp.WrapError(err, "OutputRanges", za0003, za0005)
							return
						}
						switch msgp.UnsafeString(field) {
						case "s":
							za0004[za0005].Start, err = dc.ReadUint64()
							if err != nil {
								err = msgp.WrapError(err, "OutputRanges", za0003, za0005, "Start")
								return
							}
						case "sNum":
							za0004[za0005].SubStreamNum, err = dc.ReadUint8()
							if err != nil {
								err = msgp.WrapError(err, "OutputRanges", za0003, za0005, "SubStreamNum")
								return
							}
						default:
							err = dc.Skip()
							if err != nil {
								err = msgp.WrapError(err, "OutputRanges", za0003, za0005)
								return
							}
						}
					}
				}
				z.OutputRanges[za0003] = za0004
			}
		case "startTime":
			z.StartTime, err = dc.ReadInt64()
			if err != nil {
				err = msgp.WrapError(err, "StartTime")
				return
			}
		case "sepoch":
			z.ScaleEpoch, err = dc.ReadUint16()
			if err != nil {
				err = msgp.WrapError(err, "ScaleEpoch")
				return
			}
		case "prodIndex":
			z.ProdIndex, err = dc.ReadUint8()
			if err != nil {
				err = msgp.WrapError(err, "ProdIndex")
				return
			}
		case "mark":
			{
				var zb0006 uint8
				zb0006, err = dc.ReadUint8()
				if err != nil {
					err = msgp.WrapError(err, "Mark")
					return
				}
				z.Mark = EpochMark(zb0006)
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
func (z *EpochMarker) EncodeMsg(en *msgp.Writer) (err error) {
	// omitempty: check for empty values
	zb0001Len := uint32(6)
	var zb0001Mask uint8 /* 6 bits */
	if z.ConSeqNums == nil {
		zb0001Len--
		zb0001Mask |= 0x1
	}
	if z.OutputRanges == nil {
		zb0001Len--
		zb0001Mask |= 0x2
	}
	if z.StartTime == 0 {
		zb0001Len--
		zb0001Mask |= 0x4
	}
	if z.ScaleEpoch == 0 {
		zb0001Len--
		zb0001Mask |= 0x8
	}
	if z.ProdIndex == 0 {
		zb0001Len--
		zb0001Mask |= 0x10
	}
	if z.Mark == 0 {
		zb0001Len--
		zb0001Mask |= 0x20
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
		// write "ConSeqNum"
		err = en.Append(0xa9, 0x43, 0x6f, 0x6e, 0x53, 0x65, 0x71, 0x4e, 0x75, 0x6d)
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
	}
	if (zb0001Mask & 0x2) == 0 { // if not empty
		// write "outRng"
		err = en.Append(0xa6, 0x6f, 0x75, 0x74, 0x52, 0x6e, 0x67)
		if err != nil {
			return
		}
		err = en.WriteMapHeader(uint32(len(z.OutputRanges)))
		if err != nil {
			err = msgp.WrapError(err, "OutputRanges")
			return
		}
		for za0003, za0004 := range z.OutputRanges {
			err = en.WriteString(za0003)
			if err != nil {
				err = msgp.WrapError(err, "OutputRanges")
				return
			}
			err = en.WriteArrayHeader(uint32(len(za0004)))
			if err != nil {
				err = msgp.WrapError(err, "OutputRanges", za0003)
				return
			}
			for za0005 := range za0004 {
				// map header, size 2
				// write "s"
				err = en.Append(0x82, 0xa1, 0x73)
				if err != nil {
					return
				}
				err = en.WriteUint64(za0004[za0005].Start)
				if err != nil {
					err = msgp.WrapError(err, "OutputRanges", za0003, za0005, "Start")
					return
				}
				// write "sNum"
				err = en.Append(0xa4, 0x73, 0x4e, 0x75, 0x6d)
				if err != nil {
					return
				}
				err = en.WriteUint8(za0004[za0005].SubStreamNum)
				if err != nil {
					err = msgp.WrapError(err, "OutputRanges", za0003, za0005, "SubStreamNum")
					return
				}
			}
		}
	}
	if (zb0001Mask & 0x4) == 0 { // if not empty
		// write "startTime"
		err = en.Append(0xa9, 0x73, 0x74, 0x61, 0x72, 0x74, 0x54, 0x69, 0x6d, 0x65)
		if err != nil {
			return
		}
		err = en.WriteInt64(z.StartTime)
		if err != nil {
			err = msgp.WrapError(err, "StartTime")
			return
		}
	}
	if (zb0001Mask & 0x8) == 0 { // if not empty
		// write "sepoch"
		err = en.Append(0xa6, 0x73, 0x65, 0x70, 0x6f, 0x63, 0x68)
		if err != nil {
			return
		}
		err = en.WriteUint16(z.ScaleEpoch)
		if err != nil {
			err = msgp.WrapError(err, "ScaleEpoch")
			return
		}
	}
	if (zb0001Mask & 0x10) == 0 { // if not empty
		// write "prodIndex"
		err = en.Append(0xa9, 0x70, 0x72, 0x6f, 0x64, 0x49, 0x6e, 0x64, 0x65, 0x78)
		if err != nil {
			return
		}
		err = en.WriteUint8(z.ProdIndex)
		if err != nil {
			err = msgp.WrapError(err, "ProdIndex")
			return
		}
	}
	if (zb0001Mask & 0x20) == 0 { // if not empty
		// write "mark"
		err = en.Append(0xa4, 0x6d, 0x61, 0x72, 0x6b)
		if err != nil {
			return
		}
		err = en.WriteUint8(uint8(z.Mark))
		if err != nil {
			err = msgp.WrapError(err, "Mark")
			return
		}
	}
	return
}

// MarshalMsg implements msgp.Marshaler
func (z *EpochMarker) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	// omitempty: check for empty values
	zb0001Len := uint32(6)
	var zb0001Mask uint8 /* 6 bits */
	if z.ConSeqNums == nil {
		zb0001Len--
		zb0001Mask |= 0x1
	}
	if z.OutputRanges == nil {
		zb0001Len--
		zb0001Mask |= 0x2
	}
	if z.StartTime == 0 {
		zb0001Len--
		zb0001Mask |= 0x4
	}
	if z.ScaleEpoch == 0 {
		zb0001Len--
		zb0001Mask |= 0x8
	}
	if z.ProdIndex == 0 {
		zb0001Len--
		zb0001Mask |= 0x10
	}
	if z.Mark == 0 {
		zb0001Len--
		zb0001Mask |= 0x20
	}
	// variable map header, size zb0001Len
	o = append(o, 0x80|uint8(zb0001Len))
	if zb0001Len == 0 {
		return
	}
	if (zb0001Mask & 0x1) == 0 { // if not empty
		// string "ConSeqNum"
		o = append(o, 0xa9, 0x43, 0x6f, 0x6e, 0x53, 0x65, 0x71, 0x4e, 0x75, 0x6d)
		o = msgp.AppendMapHeader(o, uint32(len(z.ConSeqNums)))
		for za0001, za0002 := range z.ConSeqNums {
			o = msgp.AppendString(o, za0001)
			o = msgp.AppendUint64(o, za0002)
		}
	}
	if (zb0001Mask & 0x2) == 0 { // if not empty
		// string "outRng"
		o = append(o, 0xa6, 0x6f, 0x75, 0x74, 0x52, 0x6e, 0x67)
		o = msgp.AppendMapHeader(o, uint32(len(z.OutputRanges)))
		for za0003, za0004 := range z.OutputRanges {
			o = msgp.AppendString(o, za0003)
			o = msgp.AppendArrayHeader(o, uint32(len(za0004)))
			for za0005 := range za0004 {
				// map header, size 2
				// string "s"
				o = append(o, 0x82, 0xa1, 0x73)
				o = msgp.AppendUint64(o, za0004[za0005].Start)
				// string "sNum"
				o = append(o, 0xa4, 0x73, 0x4e, 0x75, 0x6d)
				o = msgp.AppendUint8(o, za0004[za0005].SubStreamNum)
			}
		}
	}
	if (zb0001Mask & 0x4) == 0 { // if not empty
		// string "startTime"
		o = append(o, 0xa9, 0x73, 0x74, 0x61, 0x72, 0x74, 0x54, 0x69, 0x6d, 0x65)
		o = msgp.AppendInt64(o, z.StartTime)
	}
	if (zb0001Mask & 0x8) == 0 { // if not empty
		// string "sepoch"
		o = append(o, 0xa6, 0x73, 0x65, 0x70, 0x6f, 0x63, 0x68)
		o = msgp.AppendUint16(o, z.ScaleEpoch)
	}
	if (zb0001Mask & 0x10) == 0 { // if not empty
		// string "prodIndex"
		o = append(o, 0xa9, 0x70, 0x72, 0x6f, 0x64, 0x49, 0x6e, 0x64, 0x65, 0x78)
		o = msgp.AppendUint8(o, z.ProdIndex)
	}
	if (zb0001Mask & 0x20) == 0 { // if not empty
		// string "mark"
		o = append(o, 0xa4, 0x6d, 0x61, 0x72, 0x6b)
		o = msgp.AppendUint8(o, uint8(z.Mark))
	}
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *EpochMarker) UnmarshalMsg(bts []byte) (o []byte, err error) {
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
		case "ConSeqNum":
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
		case "outRng":
			var zb0003 uint32
			zb0003, bts, err = msgp.ReadMapHeaderBytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "OutputRanges")
				return
			}
			if z.OutputRanges == nil {
				z.OutputRanges = make(map[string][]ProduceRange, zb0003)
			} else if len(z.OutputRanges) > 0 {
				for key := range z.OutputRanges {
					delete(z.OutputRanges, key)
				}
			}
			for zb0003 > 0 {
				var za0003 string
				var za0004 []ProduceRange
				zb0003--
				za0003, bts, err = msgp.ReadStringBytes(bts)
				if err != nil {
					err = msgp.WrapError(err, "OutputRanges")
					return
				}
				var zb0004 uint32
				zb0004, bts, err = msgp.ReadArrayHeaderBytes(bts)
				if err != nil {
					err = msgp.WrapError(err, "OutputRanges", za0003)
					return
				}
				if cap(za0004) >= int(zb0004) {
					za0004 = (za0004)[:zb0004]
				} else {
					za0004 = make([]ProduceRange, zb0004)
				}
				for za0005 := range za0004 {
					var zb0005 uint32
					zb0005, bts, err = msgp.ReadMapHeaderBytes(bts)
					if err != nil {
						err = msgp.WrapError(err, "OutputRanges", za0003, za0005)
						return
					}
					for zb0005 > 0 {
						zb0005--
						field, bts, err = msgp.ReadMapKeyZC(bts)
						if err != nil {
							err = msgp.WrapError(err, "OutputRanges", za0003, za0005)
							return
						}
						switch msgp.UnsafeString(field) {
						case "s":
							za0004[za0005].Start, bts, err = msgp.ReadUint64Bytes(bts)
							if err != nil {
								err = msgp.WrapError(err, "OutputRanges", za0003, za0005, "Start")
								return
							}
						case "sNum":
							za0004[za0005].SubStreamNum, bts, err = msgp.ReadUint8Bytes(bts)
							if err != nil {
								err = msgp.WrapError(err, "OutputRanges", za0003, za0005, "SubStreamNum")
								return
							}
						default:
							bts, err = msgp.Skip(bts)
							if err != nil {
								err = msgp.WrapError(err, "OutputRanges", za0003, za0005)
								return
							}
						}
					}
				}
				z.OutputRanges[za0003] = za0004
			}
		case "startTime":
			z.StartTime, bts, err = msgp.ReadInt64Bytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "StartTime")
				return
			}
		case "sepoch":
			z.ScaleEpoch, bts, err = msgp.ReadUint16Bytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "ScaleEpoch")
				return
			}
		case "prodIndex":
			z.ProdIndex, bts, err = msgp.ReadUint8Bytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "ProdIndex")
				return
			}
		case "mark":
			{
				var zb0006 uint8
				zb0006, bts, err = msgp.ReadUint8Bytes(bts)
				if err != nil {
					err = msgp.WrapError(err, "Mark")
					return
				}
				z.Mark = EpochMark(zb0006)
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
func (z *EpochMarker) Msgsize() (s int) {
	s = 1 + 10 + msgp.MapHeaderSize
	if z.ConSeqNums != nil {
		for za0001, za0002 := range z.ConSeqNums {
			_ = za0002
			s += msgp.StringPrefixSize + len(za0001) + msgp.Uint64Size
		}
	}
	s += 7 + msgp.MapHeaderSize
	if z.OutputRanges != nil {
		for za0003, za0004 := range z.OutputRanges {
			_ = za0004
			s += msgp.StringPrefixSize + len(za0003) + msgp.ArrayHeaderSize + (len(za0004) * (8 + msgp.Uint64Size + msgp.Uint8Size))
		}
	}
	s += 10 + msgp.Int64Size + 7 + msgp.Uint16Size + 10 + msgp.Uint8Size + 5 + msgp.Uint8Size
	return
}

// DecodeMsg implements msgp.Decodable
func (z *EpochMarkerJSONSerde) DecodeMsg(dc *msgp.Reader) (err error) {
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
func (z EpochMarkerJSONSerde) EncodeMsg(en *msgp.Writer) (err error) {
	// map header, size 0
	err = en.Append(0x80)
	if err != nil {
		return
	}
	return
}

// MarshalMsg implements msgp.Marshaler
func (z EpochMarkerJSONSerde) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	// map header, size 0
	o = append(o, 0x80)
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *EpochMarkerJSONSerde) UnmarshalMsg(bts []byte) (o []byte, err error) {
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
func (z EpochMarkerJSONSerde) Msgsize() (s int) {
	s = 1
	return
}

// DecodeMsg implements msgp.Decodable
func (z *EpochMarkerMsgpSerde) DecodeMsg(dc *msgp.Reader) (err error) {
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
func (z EpochMarkerMsgpSerde) EncodeMsg(en *msgp.Writer) (err error) {
	// map header, size 0
	err = en.Append(0x80)
	if err != nil {
		return
	}
	return
}

// MarshalMsg implements msgp.Marshaler
func (z EpochMarkerMsgpSerde) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	// map header, size 0
	o = append(o, 0x80)
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *EpochMarkerMsgpSerde) UnmarshalMsg(bts []byte) (o []byte, err error) {
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
func (z EpochMarkerMsgpSerde) Msgsize() (s int) {
	s = 1
	return
}

// DecodeMsg implements msgp.Decodable
func (z *ProduceRange) DecodeMsg(dc *msgp.Reader) (err error) {
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
		case "s":
			z.Start, err = dc.ReadUint64()
			if err != nil {
				err = msgp.WrapError(err, "Start")
				return
			}
		case "sNum":
			z.SubStreamNum, err = dc.ReadUint8()
			if err != nil {
				err = msgp.WrapError(err, "SubStreamNum")
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
func (z ProduceRange) EncodeMsg(en *msgp.Writer) (err error) {
	// map header, size 2
	// write "s"
	err = en.Append(0x82, 0xa1, 0x73)
	if err != nil {
		return
	}
	err = en.WriteUint64(z.Start)
	if err != nil {
		err = msgp.WrapError(err, "Start")
		return
	}
	// write "sNum"
	err = en.Append(0xa4, 0x73, 0x4e, 0x75, 0x6d)
	if err != nil {
		return
	}
	err = en.WriteUint8(z.SubStreamNum)
	if err != nil {
		err = msgp.WrapError(err, "SubStreamNum")
		return
	}
	return
}

// MarshalMsg implements msgp.Marshaler
func (z ProduceRange) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	// map header, size 2
	// string "s"
	o = append(o, 0x82, 0xa1, 0x73)
	o = msgp.AppendUint64(o, z.Start)
	// string "sNum"
	o = append(o, 0xa4, 0x73, 0x4e, 0x75, 0x6d)
	o = msgp.AppendUint8(o, z.SubStreamNum)
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *ProduceRange) UnmarshalMsg(bts []byte) (o []byte, err error) {
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
		case "s":
			z.Start, bts, err = msgp.ReadUint64Bytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "Start")
				return
			}
		case "sNum":
			z.SubStreamNum, bts, err = msgp.ReadUint8Bytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "SubStreamNum")
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
func (z ProduceRange) Msgsize() (s int) {
	s = 1 + 2 + msgp.Uint64Size + 5 + msgp.Uint8Size
	return
}

// DecodeMsg implements msgp.Decodable
func (z *ProduceRangeWithEnd) DecodeMsg(dc *msgp.Reader) (err error) {
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
		case "End":
			z.End, err = dc.ReadUint64()
			if err != nil {
				err = msgp.WrapError(err, "End")
				return
			}
		case "ProduceRange":
			var zb0002 uint32
			zb0002, err = dc.ReadMapHeader()
			if err != nil {
				err = msgp.WrapError(err, "ProduceRange")
				return
			}
			for zb0002 > 0 {
				zb0002--
				field, err = dc.ReadMapKeyPtr()
				if err != nil {
					err = msgp.WrapError(err, "ProduceRange")
					return
				}
				switch msgp.UnsafeString(field) {
				case "s":
					z.ProduceRange.Start, err = dc.ReadUint64()
					if err != nil {
						err = msgp.WrapError(err, "ProduceRange", "Start")
						return
					}
				case "sNum":
					z.ProduceRange.SubStreamNum, err = dc.ReadUint8()
					if err != nil {
						err = msgp.WrapError(err, "ProduceRange", "SubStreamNum")
						return
					}
				default:
					err = dc.Skip()
					if err != nil {
						err = msgp.WrapError(err, "ProduceRange")
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
func (z *ProduceRangeWithEnd) EncodeMsg(en *msgp.Writer) (err error) {
	// map header, size 2
	// write "End"
	err = en.Append(0x82, 0xa3, 0x45, 0x6e, 0x64)
	if err != nil {
		return
	}
	err = en.WriteUint64(z.End)
	if err != nil {
		err = msgp.WrapError(err, "End")
		return
	}
	// write "ProduceRange"
	err = en.Append(0xac, 0x50, 0x72, 0x6f, 0x64, 0x75, 0x63, 0x65, 0x52, 0x61, 0x6e, 0x67, 0x65)
	if err != nil {
		return
	}
	// map header, size 2
	// write "s"
	err = en.Append(0x82, 0xa1, 0x73)
	if err != nil {
		return
	}
	err = en.WriteUint64(z.ProduceRange.Start)
	if err != nil {
		err = msgp.WrapError(err, "ProduceRange", "Start")
		return
	}
	// write "sNum"
	err = en.Append(0xa4, 0x73, 0x4e, 0x75, 0x6d)
	if err != nil {
		return
	}
	err = en.WriteUint8(z.ProduceRange.SubStreamNum)
	if err != nil {
		err = msgp.WrapError(err, "ProduceRange", "SubStreamNum")
		return
	}
	return
}

// MarshalMsg implements msgp.Marshaler
func (z *ProduceRangeWithEnd) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	// map header, size 2
	// string "End"
	o = append(o, 0x82, 0xa3, 0x45, 0x6e, 0x64)
	o = msgp.AppendUint64(o, z.End)
	// string "ProduceRange"
	o = append(o, 0xac, 0x50, 0x72, 0x6f, 0x64, 0x75, 0x63, 0x65, 0x52, 0x61, 0x6e, 0x67, 0x65)
	// map header, size 2
	// string "s"
	o = append(o, 0x82, 0xa1, 0x73)
	o = msgp.AppendUint64(o, z.ProduceRange.Start)
	// string "sNum"
	o = append(o, 0xa4, 0x73, 0x4e, 0x75, 0x6d)
	o = msgp.AppendUint8(o, z.ProduceRange.SubStreamNum)
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *ProduceRangeWithEnd) UnmarshalMsg(bts []byte) (o []byte, err error) {
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
		case "End":
			z.End, bts, err = msgp.ReadUint64Bytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "End")
				return
			}
		case "ProduceRange":
			var zb0002 uint32
			zb0002, bts, err = msgp.ReadMapHeaderBytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "ProduceRange")
				return
			}
			for zb0002 > 0 {
				zb0002--
				field, bts, err = msgp.ReadMapKeyZC(bts)
				if err != nil {
					err = msgp.WrapError(err, "ProduceRange")
					return
				}
				switch msgp.UnsafeString(field) {
				case "s":
					z.ProduceRange.Start, bts, err = msgp.ReadUint64Bytes(bts)
					if err != nil {
						err = msgp.WrapError(err, "ProduceRange", "Start")
						return
					}
				case "sNum":
					z.ProduceRange.SubStreamNum, bts, err = msgp.ReadUint8Bytes(bts)
					if err != nil {
						err = msgp.WrapError(err, "ProduceRange", "SubStreamNum")
						return
					}
				default:
					bts, err = msgp.Skip(bts)
					if err != nil {
						err = msgp.WrapError(err, "ProduceRange")
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
func (z *ProduceRangeWithEnd) Msgsize() (s int) {
	s = 1 + 4 + msgp.Uint64Size + 13 + 1 + 2 + msgp.Uint64Size + 5 + msgp.Uint8Size
	return
}
