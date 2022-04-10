package sharedlog_stream

// Code generated by github.com/tinylib/msgp DO NOT EDIT.

import (
	"github.com/tinylib/msgp/msgp"
)

// DecodeMsg implements msgp.Decodable
func (z *StreamLogEntry) DecodeMsg(dc *msgp.Reader) (err error) {
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
		case "topicName":
			z.TopicName, err = dc.ReadString()
			if err != nil {
				err = msgp.WrapError(err, "TopicName")
				return
			}
		case "payload":
			z.Payload, err = dc.ReadBytes(z.Payload)
			if err != nil {
				err = msgp.WrapError(err, "Payload")
				return
			}
		case "tid":
			z.TaskId, err = dc.ReadUint64()
			if err != nil {
				err = msgp.WrapError(err, "TaskId")
				return
			}
		case "mseq":
			z.MsgSeqNum, err = dc.ReadUint64()
			if err != nil {
				err = msgp.WrapError(err, "MsgSeqNum")
				return
			}
		case "te":
			z.TaskEpoch, err = dc.ReadUint16()
			if err != nil {
				err = msgp.WrapError(err, "TaskEpoch")
				return
			}
		case "meta":
			z.Meta, err = dc.ReadUint8()
			if err != nil {
				err = msgp.WrapError(err, "Meta")
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
func (z *StreamLogEntry) EncodeMsg(en *msgp.Writer) (err error) {
	// omitempty: check for empty values
	zb0001Len := uint32(6)
	var zb0001Mask uint8 /* 6 bits */
	if z.Payload == nil {
		zb0001Len--
		zb0001Mask |= 0x2
	}
	if z.TaskId == 0 {
		zb0001Len--
		zb0001Mask |= 0x4
	}
	if z.MsgSeqNum == 0 {
		zb0001Len--
		zb0001Mask |= 0x8
	}
	if z.TaskEpoch == 0 {
		zb0001Len--
		zb0001Mask |= 0x10
	}
	if z.Meta == 0 {
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
	// write "topicName"
	err = en.Append(0xa9, 0x74, 0x6f, 0x70, 0x69, 0x63, 0x4e, 0x61, 0x6d, 0x65)
	if err != nil {
		return
	}
	err = en.WriteString(z.TopicName)
	if err != nil {
		err = msgp.WrapError(err, "TopicName")
		return
	}
	if (zb0001Mask & 0x2) == 0 { // if not empty
		// write "payload"
		err = en.Append(0xa7, 0x70, 0x61, 0x79, 0x6c, 0x6f, 0x61, 0x64)
		if err != nil {
			return
		}
		err = en.WriteBytes(z.Payload)
		if err != nil {
			err = msgp.WrapError(err, "Payload")
			return
		}
	}
	if (zb0001Mask & 0x4) == 0 { // if not empty
		// write "tid"
		err = en.Append(0xa3, 0x74, 0x69, 0x64)
		if err != nil {
			return
		}
		err = en.WriteUint64(z.TaskId)
		if err != nil {
			err = msgp.WrapError(err, "TaskId")
			return
		}
	}
	if (zb0001Mask & 0x8) == 0 { // if not empty
		// write "mseq"
		err = en.Append(0xa4, 0x6d, 0x73, 0x65, 0x71)
		if err != nil {
			return
		}
		err = en.WriteUint64(z.MsgSeqNum)
		if err != nil {
			err = msgp.WrapError(err, "MsgSeqNum")
			return
		}
	}
	if (zb0001Mask & 0x10) == 0 { // if not empty
		// write "te"
		err = en.Append(0xa2, 0x74, 0x65)
		if err != nil {
			return
		}
		err = en.WriteUint16(z.TaskEpoch)
		if err != nil {
			err = msgp.WrapError(err, "TaskEpoch")
			return
		}
	}
	if (zb0001Mask & 0x20) == 0 { // if not empty
		// write "meta"
		err = en.Append(0xa4, 0x6d, 0x65, 0x74, 0x61)
		if err != nil {
			return
		}
		err = en.WriteUint8(z.Meta)
		if err != nil {
			err = msgp.WrapError(err, "Meta")
			return
		}
	}
	return
}

// MarshalMsg implements msgp.Marshaler
func (z *StreamLogEntry) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	// omitempty: check for empty values
	zb0001Len := uint32(6)
	var zb0001Mask uint8 /* 6 bits */
	if z.Payload == nil {
		zb0001Len--
		zb0001Mask |= 0x2
	}
	if z.TaskId == 0 {
		zb0001Len--
		zb0001Mask |= 0x4
	}
	if z.MsgSeqNum == 0 {
		zb0001Len--
		zb0001Mask |= 0x8
	}
	if z.TaskEpoch == 0 {
		zb0001Len--
		zb0001Mask |= 0x10
	}
	if z.Meta == 0 {
		zb0001Len--
		zb0001Mask |= 0x20
	}
	// variable map header, size zb0001Len
	o = append(o, 0x80|uint8(zb0001Len))
	if zb0001Len == 0 {
		return
	}
	// string "topicName"
	o = append(o, 0xa9, 0x74, 0x6f, 0x70, 0x69, 0x63, 0x4e, 0x61, 0x6d, 0x65)
	o = msgp.AppendString(o, z.TopicName)
	if (zb0001Mask & 0x2) == 0 { // if not empty
		// string "payload"
		o = append(o, 0xa7, 0x70, 0x61, 0x79, 0x6c, 0x6f, 0x61, 0x64)
		o = msgp.AppendBytes(o, z.Payload)
	}
	if (zb0001Mask & 0x4) == 0 { // if not empty
		// string "tid"
		o = append(o, 0xa3, 0x74, 0x69, 0x64)
		o = msgp.AppendUint64(o, z.TaskId)
	}
	if (zb0001Mask & 0x8) == 0 { // if not empty
		// string "mseq"
		o = append(o, 0xa4, 0x6d, 0x73, 0x65, 0x71)
		o = msgp.AppendUint64(o, z.MsgSeqNum)
	}
	if (zb0001Mask & 0x10) == 0 { // if not empty
		// string "te"
		o = append(o, 0xa2, 0x74, 0x65)
		o = msgp.AppendUint16(o, z.TaskEpoch)
	}
	if (zb0001Mask & 0x20) == 0 { // if not empty
		// string "meta"
		o = append(o, 0xa4, 0x6d, 0x65, 0x74, 0x61)
		o = msgp.AppendUint8(o, z.Meta)
	}
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *StreamLogEntry) UnmarshalMsg(bts []byte) (o []byte, err error) {
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
		case "topicName":
			z.TopicName, bts, err = msgp.ReadStringBytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "TopicName")
				return
			}
		case "payload":
			z.Payload, bts, err = msgp.ReadBytesBytes(bts, z.Payload)
			if err != nil {
				err = msgp.WrapError(err, "Payload")
				return
			}
		case "tid":
			z.TaskId, bts, err = msgp.ReadUint64Bytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "TaskId")
				return
			}
		case "mseq":
			z.MsgSeqNum, bts, err = msgp.ReadUint64Bytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "MsgSeqNum")
				return
			}
		case "te":
			z.TaskEpoch, bts, err = msgp.ReadUint16Bytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "TaskEpoch")
				return
			}
		case "meta":
			z.Meta, bts, err = msgp.ReadUint8Bytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "Meta")
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
func (z *StreamLogEntry) Msgsize() (s int) {
	s = 1 + 10 + msgp.StringPrefixSize + len(z.TopicName) + 8 + msgp.BytesPrefixSize + len(z.Payload) + 4 + msgp.Uint64Size + 5 + msgp.Uint64Size + 3 + msgp.Uint16Size + 5 + msgp.Uint8Size
	return
}
