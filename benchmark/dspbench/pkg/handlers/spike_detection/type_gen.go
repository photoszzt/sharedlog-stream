package spike_detection

// Code generated by github.com/tinylib/msgp DO NOT EDIT.

import (
	"github.com/tinylib/msgp/msgp"
)

// DecodeMsg implements msgp.Decodable
func (z *SensorData) DecodeMsg(dc *msgp.Reader) (err error) {
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
		case "val":
			z.Val, err = dc.ReadFloat64()
			if err != nil {
				err = msgp.WrapError(err, "Val")
				return
			}
		case "ts":
			z.Timestamp, err = dc.ReadUint64()
			if err != nil {
				err = msgp.WrapError(err, "Timestamp")
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
func (z SensorData) EncodeMsg(en *msgp.Writer) (err error) {
	// map header, size 2
	// write "val"
	err = en.Append(0x82, 0xa3, 0x76, 0x61, 0x6c)
	if err != nil {
		return
	}
	err = en.WriteFloat64(z.Val)
	if err != nil {
		err = msgp.WrapError(err, "Val")
		return
	}
	// write "ts"
	err = en.Append(0xa2, 0x74, 0x73)
	if err != nil {
		return
	}
	err = en.WriteUint64(z.Timestamp)
	if err != nil {
		err = msgp.WrapError(err, "Timestamp")
		return
	}
	return
}

// MarshalMsg implements msgp.Marshaler
func (z SensorData) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	// map header, size 2
	// string "val"
	o = append(o, 0x82, 0xa3, 0x76, 0x61, 0x6c)
	o = msgp.AppendFloat64(o, z.Val)
	// string "ts"
	o = append(o, 0xa2, 0x74, 0x73)
	o = msgp.AppendUint64(o, z.Timestamp)
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *SensorData) UnmarshalMsg(bts []byte) (o []byte, err error) {
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
		case "val":
			z.Val, bts, err = msgp.ReadFloat64Bytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "Val")
				return
			}
		case "ts":
			z.Timestamp, bts, err = msgp.ReadUint64Bytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "Timestamp")
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
func (z SensorData) Msgsize() (s int) {
	s = 1 + 4 + msgp.Float64Size + 3 + msgp.Uint64Size
	return
}

// DecodeMsg implements msgp.Decodable
func (z *SensorDataJSONSerde) DecodeMsg(dc *msgp.Reader) (err error) {
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
func (z SensorDataJSONSerde) EncodeMsg(en *msgp.Writer) (err error) {
	// map header, size 0
	err = en.Append(0x80)
	if err != nil {
		return
	}
	return
}

// MarshalMsg implements msgp.Marshaler
func (z SensorDataJSONSerde) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	// map header, size 0
	o = append(o, 0x80)
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *SensorDataJSONSerde) UnmarshalMsg(bts []byte) (o []byte, err error) {
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
func (z SensorDataJSONSerde) Msgsize() (s int) {
	s = 1
	return
}

// DecodeMsg implements msgp.Decodable
func (z *SensorDataMsgpSerde) DecodeMsg(dc *msgp.Reader) (err error) {
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
func (z SensorDataMsgpSerde) EncodeMsg(en *msgp.Writer) (err error) {
	// map header, size 0
	err = en.Append(0x80)
	if err != nil {
		return
	}
	return
}

// MarshalMsg implements msgp.Marshaler
func (z SensorDataMsgpSerde) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	// map header, size 0
	o = append(o, 0x80)
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *SensorDataMsgpSerde) UnmarshalMsg(bts []byte) (o []byte, err error) {
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
func (z SensorDataMsgpSerde) Msgsize() (s int) {
	s = 1
	return
}

// DecodeMsg implements msgp.Decodable
func (z *SumAndHist) DecodeMsg(dc *msgp.Reader) (err error) {
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
		case "hist":
			var zb0002 uint32
			zb0002, err = dc.ReadArrayHeader()
			if err != nil {
				err = msgp.WrapError(err, "History")
				return
			}
			if cap(z.History) >= int(zb0002) {
				z.History = (z.History)[:zb0002]
			} else {
				z.History = make([]float64, zb0002)
			}
			for za0001 := range z.History {
				z.History[za0001], err = dc.ReadFloat64()
				if err != nil {
					err = msgp.WrapError(err, "History", za0001)
					return
				}
			}
		case "val":
			z.Val, err = dc.ReadFloat64()
			if err != nil {
				err = msgp.WrapError(err, "Val")
				return
			}
		case "sum":
			z.Sum, err = dc.ReadFloat64()
			if err != nil {
				err = msgp.WrapError(err, "Sum")
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
func (z *SumAndHist) EncodeMsg(en *msgp.Writer) (err error) {
	// map header, size 3
	// write "hist"
	err = en.Append(0x83, 0xa4, 0x68, 0x69, 0x73, 0x74)
	if err != nil {
		return
	}
	err = en.WriteArrayHeader(uint32(len(z.History)))
	if err != nil {
		err = msgp.WrapError(err, "History")
		return
	}
	for za0001 := range z.History {
		err = en.WriteFloat64(z.History[za0001])
		if err != nil {
			err = msgp.WrapError(err, "History", za0001)
			return
		}
	}
	// write "val"
	err = en.Append(0xa3, 0x76, 0x61, 0x6c)
	if err != nil {
		return
	}
	err = en.WriteFloat64(z.Val)
	if err != nil {
		err = msgp.WrapError(err, "Val")
		return
	}
	// write "sum"
	err = en.Append(0xa3, 0x73, 0x75, 0x6d)
	if err != nil {
		return
	}
	err = en.WriteFloat64(z.Sum)
	if err != nil {
		err = msgp.WrapError(err, "Sum")
		return
	}
	return
}

// MarshalMsg implements msgp.Marshaler
func (z *SumAndHist) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	// map header, size 3
	// string "hist"
	o = append(o, 0x83, 0xa4, 0x68, 0x69, 0x73, 0x74)
	o = msgp.AppendArrayHeader(o, uint32(len(z.History)))
	for za0001 := range z.History {
		o = msgp.AppendFloat64(o, z.History[za0001])
	}
	// string "val"
	o = append(o, 0xa3, 0x76, 0x61, 0x6c)
	o = msgp.AppendFloat64(o, z.Val)
	// string "sum"
	o = append(o, 0xa3, 0x73, 0x75, 0x6d)
	o = msgp.AppendFloat64(o, z.Sum)
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *SumAndHist) UnmarshalMsg(bts []byte) (o []byte, err error) {
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
		case "hist":
			var zb0002 uint32
			zb0002, bts, err = msgp.ReadArrayHeaderBytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "History")
				return
			}
			if cap(z.History) >= int(zb0002) {
				z.History = (z.History)[:zb0002]
			} else {
				z.History = make([]float64, zb0002)
			}
			for za0001 := range z.History {
				z.History[za0001], bts, err = msgp.ReadFloat64Bytes(bts)
				if err != nil {
					err = msgp.WrapError(err, "History", za0001)
					return
				}
			}
		case "val":
			z.Val, bts, err = msgp.ReadFloat64Bytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "Val")
				return
			}
		case "sum":
			z.Sum, bts, err = msgp.ReadFloat64Bytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "Sum")
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
func (z *SumAndHist) Msgsize() (s int) {
	s = 1 + 5 + msgp.ArrayHeaderSize + (len(z.History) * (msgp.Float64Size)) + 4 + msgp.Float64Size + 4 + msgp.Float64Size
	return
}

// DecodeMsg implements msgp.Decodable
func (z *SumAndHistJSONSerde) DecodeMsg(dc *msgp.Reader) (err error) {
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
func (z SumAndHistJSONSerde) EncodeMsg(en *msgp.Writer) (err error) {
	// map header, size 0
	err = en.Append(0x80)
	if err != nil {
		return
	}
	return
}

// MarshalMsg implements msgp.Marshaler
func (z SumAndHistJSONSerde) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	// map header, size 0
	o = append(o, 0x80)
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *SumAndHistJSONSerde) UnmarshalMsg(bts []byte) (o []byte, err error) {
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
func (z SumAndHistJSONSerde) Msgsize() (s int) {
	s = 1
	return
}

// DecodeMsg implements msgp.Decodable
func (z *SumAndHistMsgpSerde) DecodeMsg(dc *msgp.Reader) (err error) {
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
func (z SumAndHistMsgpSerde) EncodeMsg(en *msgp.Writer) (err error) {
	// map header, size 0
	err = en.Append(0x80)
	if err != nil {
		return
	}
	return
}

// MarshalMsg implements msgp.Marshaler
func (z SumAndHistMsgpSerde) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	// map header, size 0
	o = append(o, 0x80)
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *SumAndHistMsgpSerde) UnmarshalMsg(bts []byte) (o []byte, err error) {
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
func (z SumAndHistMsgpSerde) Msgsize() (s int) {
	s = 1
	return
}

// DecodeMsg implements msgp.Decodable
func (z *ValAndAvg) DecodeMsg(dc *msgp.Reader) (err error) {
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
		case "val":
			z.Val, err = dc.ReadFloat64()
			if err != nil {
				err = msgp.WrapError(err, "Val")
				return
			}
		case "avg":
			z.Avg, err = dc.ReadFloat64()
			if err != nil {
				err = msgp.WrapError(err, "Avg")
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
func (z ValAndAvg) EncodeMsg(en *msgp.Writer) (err error) {
	// map header, size 2
	// write "val"
	err = en.Append(0x82, 0xa3, 0x76, 0x61, 0x6c)
	if err != nil {
		return
	}
	err = en.WriteFloat64(z.Val)
	if err != nil {
		err = msgp.WrapError(err, "Val")
		return
	}
	// write "avg"
	err = en.Append(0xa3, 0x61, 0x76, 0x67)
	if err != nil {
		return
	}
	err = en.WriteFloat64(z.Avg)
	if err != nil {
		err = msgp.WrapError(err, "Avg")
		return
	}
	return
}

// MarshalMsg implements msgp.Marshaler
func (z ValAndAvg) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	// map header, size 2
	// string "val"
	o = append(o, 0x82, 0xa3, 0x76, 0x61, 0x6c)
	o = msgp.AppendFloat64(o, z.Val)
	// string "avg"
	o = append(o, 0xa3, 0x61, 0x76, 0x67)
	o = msgp.AppendFloat64(o, z.Avg)
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *ValAndAvg) UnmarshalMsg(bts []byte) (o []byte, err error) {
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
		case "val":
			z.Val, bts, err = msgp.ReadFloat64Bytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "Val")
				return
			}
		case "avg":
			z.Avg, bts, err = msgp.ReadFloat64Bytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "Avg")
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
func (z ValAndAvg) Msgsize() (s int) {
	s = 1 + 4 + msgp.Float64Size + 4 + msgp.Float64Size
	return
}

// DecodeMsg implements msgp.Decodable
func (z *ValAndAvgJSONSerde) DecodeMsg(dc *msgp.Reader) (err error) {
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
func (z ValAndAvgJSONSerde) EncodeMsg(en *msgp.Writer) (err error) {
	// map header, size 0
	err = en.Append(0x80)
	if err != nil {
		return
	}
	return
}

// MarshalMsg implements msgp.Marshaler
func (z ValAndAvgJSONSerde) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	// map header, size 0
	o = append(o, 0x80)
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *ValAndAvgJSONSerde) UnmarshalMsg(bts []byte) (o []byte, err error) {
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
func (z ValAndAvgJSONSerde) Msgsize() (s int) {
	s = 1
	return
}

// DecodeMsg implements msgp.Decodable
func (z *ValAndAvgMsgpSerde) DecodeMsg(dc *msgp.Reader) (err error) {
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
func (z ValAndAvgMsgpSerde) EncodeMsg(en *msgp.Writer) (err error) {
	// map header, size 0
	err = en.Append(0x80)
	if err != nil {
		return
	}
	return
}

// MarshalMsg implements msgp.Marshaler
func (z ValAndAvgMsgpSerde) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	// map header, size 0
	o = append(o, 0x80)
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *ValAndAvgMsgpSerde) UnmarshalMsg(bts []byte) (o []byte, err error) {
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
func (z ValAndAvgMsgpSerde) Msgsize() (s int) {
	s = 1
	return
}
