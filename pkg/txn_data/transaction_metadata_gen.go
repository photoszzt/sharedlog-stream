package txn_data

// Code generated by github.com/tinylib/msgp DO NOT EDIT.

import (
	"github.com/tinylib/msgp/msgp"
)

// DecodeMsg implements msgp.Decodable
func (z *TransactionState) DecodeMsg(dc *msgp.Reader) (err error) {
	{
		var zb0001 uint32
		zb0001, err = dc.ReadUint32()
		if err != nil {
			err = msgp.WrapError(err)
			return
		}
		(*z) = TransactionState(zb0001)
	}
	return
}

// EncodeMsg implements msgp.Encodable
func (z TransactionState) EncodeMsg(en *msgp.Writer) (err error) {
	err = en.WriteUint32(uint32(z))
	if err != nil {
		err = msgp.WrapError(err)
		return
	}
	return
}

// MarshalMsg implements msgp.Marshaler
func (z TransactionState) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	o = msgp.AppendUint32(o, uint32(z))
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *TransactionState) UnmarshalMsg(bts []byte) (o []byte, err error) {
	{
		var zb0001 uint32
		zb0001, bts, err = msgp.ReadUint32Bytes(bts)
		if err != nil {
			err = msgp.WrapError(err)
			return
		}
		(*z) = TransactionState(zb0001)
	}
	o = bts
	return
}

// Msgsize returns an upper bound estimate of the number of bytes occupied by the serialized message
func (z TransactionState) Msgsize() (s int) {
	s = msgp.Uint32Size
	return
}

// DecodeMsg implements msgp.Decodable
func (z *TxnMetadata) DecodeMsg(dc *msgp.Reader) (err error) {
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
		case "tp":
			var zb0002 uint32
			zb0002, err = dc.ReadArrayHeader()
			if err != nil {
				err = msgp.WrapError(err, "TopicPartitions")
				return
			}
			if cap(z.TopicPartitions) >= int(zb0002) {
				z.TopicPartitions = (z.TopicPartitions)[:zb0002]
			} else {
				z.TopicPartitions = make([]*TopicPartition, zb0002)
			}
			for za0001 := range z.TopicPartitions {
				if dc.IsNil() {
					err = dc.ReadNil()
					if err != nil {
						err = msgp.WrapError(err, "TopicPartitions", za0001)
						return
					}
					z.TopicPartitions[za0001] = nil
				} else {
					if z.TopicPartitions[za0001] == nil {
						z.TopicPartitions[za0001] = new(TopicPartition)
					}
					err = z.TopicPartitions[za0001].DecodeMsg(dc)
					if err != nil {
						err = msgp.WrapError(err, "TopicPartitions", za0001)
						return
					}
				}
			}
		case "st":
			{
				var zb0003 uint32
				zb0003, err = dc.ReadUint32()
				if err != nil {
					err = msgp.WrapError(err, "State")
					return
				}
				z.State = TransactionState(zb0003)
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
func (z *TxnMetadata) EncodeMsg(en *msgp.Writer) (err error) {
	// omitempty: check for empty values
	zb0001Len := uint32(2)
	var zb0001Mask uint8 /* 2 bits */
	_ = zb0001Mask
	if z.TopicPartitions == nil {
		zb0001Len--
		zb0001Mask |= 0x1
	}
	if z.State == 0 {
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
		// write "tp"
		err = en.Append(0xa2, 0x74, 0x70)
		if err != nil {
			return
		}
		err = en.WriteArrayHeader(uint32(len(z.TopicPartitions)))
		if err != nil {
			err = msgp.WrapError(err, "TopicPartitions")
			return
		}
		for za0001 := range z.TopicPartitions {
			if z.TopicPartitions[za0001] == nil {
				err = en.WriteNil()
				if err != nil {
					return
				}
			} else {
				err = z.TopicPartitions[za0001].EncodeMsg(en)
				if err != nil {
					err = msgp.WrapError(err, "TopicPartitions", za0001)
					return
				}
			}
		}
	}
	if (zb0001Mask & 0x2) == 0 { // if not empty
		// write "st"
		err = en.Append(0xa2, 0x73, 0x74)
		if err != nil {
			return
		}
		err = en.WriteUint32(uint32(z.State))
		if err != nil {
			err = msgp.WrapError(err, "State")
			return
		}
	}
	return
}

// MarshalMsg implements msgp.Marshaler
func (z *TxnMetadata) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	// omitempty: check for empty values
	zb0001Len := uint32(2)
	var zb0001Mask uint8 /* 2 bits */
	_ = zb0001Mask
	if z.TopicPartitions == nil {
		zb0001Len--
		zb0001Mask |= 0x1
	}
	if z.State == 0 {
		zb0001Len--
		zb0001Mask |= 0x2
	}
	// variable map header, size zb0001Len
	o = append(o, 0x80|uint8(zb0001Len))
	if zb0001Len == 0 {
		return
	}
	if (zb0001Mask & 0x1) == 0 { // if not empty
		// string "tp"
		o = append(o, 0xa2, 0x74, 0x70)
		o = msgp.AppendArrayHeader(o, uint32(len(z.TopicPartitions)))
		for za0001 := range z.TopicPartitions {
			if z.TopicPartitions[za0001] == nil {
				o = msgp.AppendNil(o)
			} else {
				o, err = z.TopicPartitions[za0001].MarshalMsg(o)
				if err != nil {
					err = msgp.WrapError(err, "TopicPartitions", za0001)
					return
				}
			}
		}
	}
	if (zb0001Mask & 0x2) == 0 { // if not empty
		// string "st"
		o = append(o, 0xa2, 0x73, 0x74)
		o = msgp.AppendUint32(o, uint32(z.State))
	}
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *TxnMetadata) UnmarshalMsg(bts []byte) (o []byte, err error) {
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
		case "tp":
			var zb0002 uint32
			zb0002, bts, err = msgp.ReadArrayHeaderBytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "TopicPartitions")
				return
			}
			if cap(z.TopicPartitions) >= int(zb0002) {
				z.TopicPartitions = (z.TopicPartitions)[:zb0002]
			} else {
				z.TopicPartitions = make([]*TopicPartition, zb0002)
			}
			for za0001 := range z.TopicPartitions {
				if msgp.IsNil(bts) {
					bts, err = msgp.ReadNilBytes(bts)
					if err != nil {
						return
					}
					z.TopicPartitions[za0001] = nil
				} else {
					if z.TopicPartitions[za0001] == nil {
						z.TopicPartitions[za0001] = new(TopicPartition)
					}
					bts, err = z.TopicPartitions[za0001].UnmarshalMsg(bts)
					if err != nil {
						err = msgp.WrapError(err, "TopicPartitions", za0001)
						return
					}
				}
			}
		case "st":
			{
				var zb0003 uint32
				zb0003, bts, err = msgp.ReadUint32Bytes(bts)
				if err != nil {
					err = msgp.WrapError(err, "State")
					return
				}
				z.State = TransactionState(zb0003)
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
func (z *TxnMetadata) Msgsize() (s int) {
	s = 1 + 3 + msgp.ArrayHeaderSize
	for za0001 := range z.TopicPartitions {
		if z.TopicPartitions[za0001] == nil {
			s += msgp.NilSize
		} else {
			s += z.TopicPartitions[za0001].Msgsize()
		}
	}
	s += 3 + msgp.Uint32Size
	return
}
