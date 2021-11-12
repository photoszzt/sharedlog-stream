package sharedlog_stream

// Code generated by github.com/tinylib/msgp DO NOT EDIT.

import (
	"github.com/tinylib/msgp/msgp"
)

// DecodeMsg implements msgp.Decodable
func (z *TopicPartition) DecodeMsg(dc *msgp.Reader) (err error) {
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
		case "topic":
			z.Topic, err = dc.ReadString()
			if err != nil {
				err = msgp.WrapError(err, "Topic")
				return
			}
		case "parnum":
			var zb0002 uint32
			zb0002, err = dc.ReadArrayHeader()
			if err != nil {
				err = msgp.WrapError(err, "ParNum")
				return
			}
			if cap(z.ParNum) >= int(zb0002) {
				z.ParNum = (z.ParNum)[:zb0002]
			} else {
				z.ParNum = make([]uint8, zb0002)
			}
			for za0001 := range z.ParNum {
				z.ParNum[za0001], err = dc.ReadUint8()
				if err != nil {
					err = msgp.WrapError(err, "ParNum", za0001)
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
func (z *TopicPartition) EncodeMsg(en *msgp.Writer) (err error) {
	// map header, size 2
	// write "topic"
	err = en.Append(0x82, 0xa5, 0x74, 0x6f, 0x70, 0x69, 0x63)
	if err != nil {
		return
	}
	err = en.WriteString(z.Topic)
	if err != nil {
		err = msgp.WrapError(err, "Topic")
		return
	}
	// write "parnum"
	err = en.Append(0xa6, 0x70, 0x61, 0x72, 0x6e, 0x75, 0x6d)
	if err != nil {
		return
	}
	err = en.WriteArrayHeader(uint32(len(z.ParNum)))
	if err != nil {
		err = msgp.WrapError(err, "ParNum")
		return
	}
	for za0001 := range z.ParNum {
		err = en.WriteUint8(z.ParNum[za0001])
		if err != nil {
			err = msgp.WrapError(err, "ParNum", za0001)
			return
		}
	}
	return
}

// MarshalMsg implements msgp.Marshaler
func (z *TopicPartition) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	// map header, size 2
	// string "topic"
	o = append(o, 0x82, 0xa5, 0x74, 0x6f, 0x70, 0x69, 0x63)
	o = msgp.AppendString(o, z.Topic)
	// string "parnum"
	o = append(o, 0xa6, 0x70, 0x61, 0x72, 0x6e, 0x75, 0x6d)
	o = msgp.AppendArrayHeader(o, uint32(len(z.ParNum)))
	for za0001 := range z.ParNum {
		o = msgp.AppendUint8(o, z.ParNum[za0001])
	}
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *TopicPartition) UnmarshalMsg(bts []byte) (o []byte, err error) {
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
		case "topic":
			z.Topic, bts, err = msgp.ReadStringBytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "Topic")
				return
			}
		case "parnum":
			var zb0002 uint32
			zb0002, bts, err = msgp.ReadArrayHeaderBytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "ParNum")
				return
			}
			if cap(z.ParNum) >= int(zb0002) {
				z.ParNum = (z.ParNum)[:zb0002]
			} else {
				z.ParNum = make([]uint8, zb0002)
			}
			for za0001 := range z.ParNum {
				z.ParNum[za0001], bts, err = msgp.ReadUint8Bytes(bts)
				if err != nil {
					err = msgp.WrapError(err, "ParNum", za0001)
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
func (z *TopicPartition) Msgsize() (s int) {
	s = 1 + 6 + msgp.StringPrefixSize + len(z.Topic) + 7 + msgp.ArrayHeaderSize + (len(z.ParNum) * (msgp.Uint8Size))
	return
}