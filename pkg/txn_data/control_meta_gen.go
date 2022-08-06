package txn_data

// Code generated by github.com/tinylib/msgp DO NOT EDIT.

import (
	"github.com/tinylib/msgp/msgp"
)

// DecodeMsg implements msgp.Decodable
func (z *ControlMetadata) DecodeMsg(dc *msgp.Reader) (err error) {
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
		case "Config":
			var zb0002 uint32
			zb0002, err = dc.ReadMapHeader()
			if err != nil {
				err = msgp.WrapError(err, "Config")
				return
			}
			if z.Config == nil {
				z.Config = make(map[string]uint8, zb0002)
			} else if len(z.Config) > 0 {
				for key := range z.Config {
					delete(z.Config, key)
				}
			}
			for zb0002 > 0 {
				zb0002--
				var za0001 string
				var za0002 uint8
				za0001, err = dc.ReadString()
				if err != nil {
					err = msgp.WrapError(err, "Config")
					return
				}
				za0002, err = dc.ReadUint8()
				if err != nil {
					err = msgp.WrapError(err, "Config", za0001)
					return
				}
				z.Config[za0001] = za0002
			}
		case "Topic":
			z.Topic, err = dc.ReadString()
			if err != nil {
				err = msgp.WrapError(err, "Topic")
				return
			}
		case "FinishedPrevTask":
			z.FinishedPrevTask, err = dc.ReadString()
			if err != nil {
				err = msgp.WrapError(err, "FinishedPrevTask")
				return
			}
		case "Key":
			z.Key, err = dc.ReadBytes(z.Key)
			if err != nil {
				err = msgp.WrapError(err, "Key")
				return
			}
		case "Hash":
			z.Hash, err = dc.ReadUint64()
			if err != nil {
				err = msgp.WrapError(err, "Hash")
				return
			}
		case "Epoch":
			z.Epoch, err = dc.ReadUint16()
			if err != nil {
				err = msgp.WrapError(err, "Epoch")
				return
			}
		case "InstanceId":
			z.InstanceId, err = dc.ReadUint8()
			if err != nil {
				err = msgp.WrapError(err, "InstanceId")
				return
			}
		case "SubstreamId":
			z.SubstreamId, err = dc.ReadUint8()
			if err != nil {
				err = msgp.WrapError(err, "SubstreamId")
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
func (z *ControlMetadata) EncodeMsg(en *msgp.Writer) (err error) {
	// map header, size 8
	// write "Config"
	err = en.Append(0x88, 0xa6, 0x43, 0x6f, 0x6e, 0x66, 0x69, 0x67)
	if err != nil {
		return
	}
	err = en.WriteMapHeader(uint32(len(z.Config)))
	if err != nil {
		err = msgp.WrapError(err, "Config")
		return
	}
	for za0001, za0002 := range z.Config {
		err = en.WriteString(za0001)
		if err != nil {
			err = msgp.WrapError(err, "Config")
			return
		}
		err = en.WriteUint8(za0002)
		if err != nil {
			err = msgp.WrapError(err, "Config", za0001)
			return
		}
	}
	// write "Topic"
	err = en.Append(0xa5, 0x54, 0x6f, 0x70, 0x69, 0x63)
	if err != nil {
		return
	}
	err = en.WriteString(z.Topic)
	if err != nil {
		err = msgp.WrapError(err, "Topic")
		return
	}
	// write "FinishedPrevTask"
	err = en.Append(0xb0, 0x46, 0x69, 0x6e, 0x69, 0x73, 0x68, 0x65, 0x64, 0x50, 0x72, 0x65, 0x76, 0x54, 0x61, 0x73, 0x6b)
	if err != nil {
		return
	}
	err = en.WriteString(z.FinishedPrevTask)
	if err != nil {
		err = msgp.WrapError(err, "FinishedPrevTask")
		return
	}
	// write "Key"
	err = en.Append(0xa3, 0x4b, 0x65, 0x79)
	if err != nil {
		return
	}
	err = en.WriteBytes(z.Key)
	if err != nil {
		err = msgp.WrapError(err, "Key")
		return
	}
	// write "Hash"
	err = en.Append(0xa4, 0x48, 0x61, 0x73, 0x68)
	if err != nil {
		return
	}
	err = en.WriteUint64(z.Hash)
	if err != nil {
		err = msgp.WrapError(err, "Hash")
		return
	}
	// write "Epoch"
	err = en.Append(0xa5, 0x45, 0x70, 0x6f, 0x63, 0x68)
	if err != nil {
		return
	}
	err = en.WriteUint16(z.Epoch)
	if err != nil {
		err = msgp.WrapError(err, "Epoch")
		return
	}
	// write "InstanceId"
	err = en.Append(0xaa, 0x49, 0x6e, 0x73, 0x74, 0x61, 0x6e, 0x63, 0x65, 0x49, 0x64)
	if err != nil {
		return
	}
	err = en.WriteUint8(z.InstanceId)
	if err != nil {
		err = msgp.WrapError(err, "InstanceId")
		return
	}
	// write "SubstreamId"
	err = en.Append(0xab, 0x53, 0x75, 0x62, 0x73, 0x74, 0x72, 0x65, 0x61, 0x6d, 0x49, 0x64)
	if err != nil {
		return
	}
	err = en.WriteUint8(z.SubstreamId)
	if err != nil {
		err = msgp.WrapError(err, "SubstreamId")
		return
	}
	return
}

// MarshalMsg implements msgp.Marshaler
func (z *ControlMetadata) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	// map header, size 8
	// string "Config"
	o = append(o, 0x88, 0xa6, 0x43, 0x6f, 0x6e, 0x66, 0x69, 0x67)
	o = msgp.AppendMapHeader(o, uint32(len(z.Config)))
	for za0001, za0002 := range z.Config {
		o = msgp.AppendString(o, za0001)
		o = msgp.AppendUint8(o, za0002)
	}
	// string "Topic"
	o = append(o, 0xa5, 0x54, 0x6f, 0x70, 0x69, 0x63)
	o = msgp.AppendString(o, z.Topic)
	// string "FinishedPrevTask"
	o = append(o, 0xb0, 0x46, 0x69, 0x6e, 0x69, 0x73, 0x68, 0x65, 0x64, 0x50, 0x72, 0x65, 0x76, 0x54, 0x61, 0x73, 0x6b)
	o = msgp.AppendString(o, z.FinishedPrevTask)
	// string "Key"
	o = append(o, 0xa3, 0x4b, 0x65, 0x79)
	o = msgp.AppendBytes(o, z.Key)
	// string "Hash"
	o = append(o, 0xa4, 0x48, 0x61, 0x73, 0x68)
	o = msgp.AppendUint64(o, z.Hash)
	// string "Epoch"
	o = append(o, 0xa5, 0x45, 0x70, 0x6f, 0x63, 0x68)
	o = msgp.AppendUint16(o, z.Epoch)
	// string "InstanceId"
	o = append(o, 0xaa, 0x49, 0x6e, 0x73, 0x74, 0x61, 0x6e, 0x63, 0x65, 0x49, 0x64)
	o = msgp.AppendUint8(o, z.InstanceId)
	// string "SubstreamId"
	o = append(o, 0xab, 0x53, 0x75, 0x62, 0x73, 0x74, 0x72, 0x65, 0x61, 0x6d, 0x49, 0x64)
	o = msgp.AppendUint8(o, z.SubstreamId)
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *ControlMetadata) UnmarshalMsg(bts []byte) (o []byte, err error) {
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
		case "Config":
			var zb0002 uint32
			zb0002, bts, err = msgp.ReadMapHeaderBytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "Config")
				return
			}
			if z.Config == nil {
				z.Config = make(map[string]uint8, zb0002)
			} else if len(z.Config) > 0 {
				for key := range z.Config {
					delete(z.Config, key)
				}
			}
			for zb0002 > 0 {
				var za0001 string
				var za0002 uint8
				zb0002--
				za0001, bts, err = msgp.ReadStringBytes(bts)
				if err != nil {
					err = msgp.WrapError(err, "Config")
					return
				}
				za0002, bts, err = msgp.ReadUint8Bytes(bts)
				if err != nil {
					err = msgp.WrapError(err, "Config", za0001)
					return
				}
				z.Config[za0001] = za0002
			}
		case "Topic":
			z.Topic, bts, err = msgp.ReadStringBytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "Topic")
				return
			}
		case "FinishedPrevTask":
			z.FinishedPrevTask, bts, err = msgp.ReadStringBytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "FinishedPrevTask")
				return
			}
		case "Key":
			z.Key, bts, err = msgp.ReadBytesBytes(bts, z.Key)
			if err != nil {
				err = msgp.WrapError(err, "Key")
				return
			}
		case "Hash":
			z.Hash, bts, err = msgp.ReadUint64Bytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "Hash")
				return
			}
		case "Epoch":
			z.Epoch, bts, err = msgp.ReadUint16Bytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "Epoch")
				return
			}
		case "InstanceId":
			z.InstanceId, bts, err = msgp.ReadUint8Bytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "InstanceId")
				return
			}
		case "SubstreamId":
			z.SubstreamId, bts, err = msgp.ReadUint8Bytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "SubstreamId")
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
func (z *ControlMetadata) Msgsize() (s int) {
	s = 1 + 7 + msgp.MapHeaderSize
	if z.Config != nil {
		for za0001, za0002 := range z.Config {
			_ = za0002
			s += msgp.StringPrefixSize + len(za0001) + msgp.Uint8Size
		}
	}
	s += 6 + msgp.StringPrefixSize + len(z.Topic) + 17 + msgp.StringPrefixSize + len(z.FinishedPrevTask) + 4 + msgp.BytesPrefixSize + len(z.Key) + 5 + msgp.Uint64Size + 6 + msgp.Uint16Size + 11 + msgp.Uint8Size + 12 + msgp.Uint8Size
	return
}
