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
		case "FinishedPrevTask":
			z.FinishedPrevTask, err = dc.ReadString()
			if err != nil {
				err = msgp.WrapError(err, "FinishedPrevTask")
				return
			}
		case "KeyMaps":
			var zb0003 uint32
			zb0003, err = dc.ReadMapHeader()
			if err != nil {
				err = msgp.WrapError(err, "KeyMaps")
				return
			}
			if z.KeyMaps == nil {
				z.KeyMaps = make(map[string][]KeyMaping, zb0003)
			} else if len(z.KeyMaps) > 0 {
				for key := range z.KeyMaps {
					delete(z.KeyMaps, key)
				}
			}
			for zb0003 > 0 {
				zb0003--
				var za0003 string
				var za0004 []KeyMaping
				za0003, err = dc.ReadString()
				if err != nil {
					err = msgp.WrapError(err, "KeyMaps")
					return
				}
				var zb0004 uint32
				zb0004, err = dc.ReadArrayHeader()
				if err != nil {
					err = msgp.WrapError(err, "KeyMaps", za0003)
					return
				}
				if cap(za0004) >= int(zb0004) {
					za0004 = (za0004)[:zb0004]
				} else {
					za0004 = make([]KeyMaping, zb0004)
				}
				for za0005 := range za0004 {
					var zb0005 uint32
					zb0005, err = dc.ReadMapHeader()
					if err != nil {
						err = msgp.WrapError(err, "KeyMaps", za0003, za0005)
						return
					}
					for zb0005 > 0 {
						zb0005--
						field, err = dc.ReadMapKeyPtr()
						if err != nil {
							err = msgp.WrapError(err, "KeyMaps", za0003, za0005)
							return
						}
						switch msgp.UnsafeString(field) {
						case "Key":
							za0004[za0005].Key, err = dc.ReadBytes(za0004[za0005].Key)
							if err != nil {
								err = msgp.WrapError(err, "KeyMaps", za0003, za0005, "Key")
								return
							}
						case "Hash":
							za0004[za0005].Hash, err = dc.ReadUint64()
							if err != nil {
								err = msgp.WrapError(err, "KeyMaps", za0003, za0005, "Hash")
								return
							}
						case "SubstreamId":
							za0004[za0005].SubstreamId, err = dc.ReadUint8()
							if err != nil {
								err = msgp.WrapError(err, "KeyMaps", za0003, za0005, "SubstreamId")
								return
							}
						default:
							err = dc.Skip()
							if err != nil {
								err = msgp.WrapError(err, "KeyMaps", za0003, za0005)
								return
							}
						}
					}
				}
				z.KeyMaps[za0003] = za0004
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
	// map header, size 5
	// write "Config"
	err = en.Append(0x85, 0xa6, 0x43, 0x6f, 0x6e, 0x66, 0x69, 0x67)
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
	// write "KeyMaps"
	err = en.Append(0xa7, 0x4b, 0x65, 0x79, 0x4d, 0x61, 0x70, 0x73)
	if err != nil {
		return
	}
	err = en.WriteMapHeader(uint32(len(z.KeyMaps)))
	if err != nil {
		err = msgp.WrapError(err, "KeyMaps")
		return
	}
	for za0003, za0004 := range z.KeyMaps {
		err = en.WriteString(za0003)
		if err != nil {
			err = msgp.WrapError(err, "KeyMaps")
			return
		}
		err = en.WriteArrayHeader(uint32(len(za0004)))
		if err != nil {
			err = msgp.WrapError(err, "KeyMaps", za0003)
			return
		}
		for za0005 := range za0004 {
			// map header, size 3
			// write "Key"
			err = en.Append(0x83, 0xa3, 0x4b, 0x65, 0x79)
			if err != nil {
				return
			}
			err = en.WriteBytes(za0004[za0005].Key)
			if err != nil {
				err = msgp.WrapError(err, "KeyMaps", za0003, za0005, "Key")
				return
			}
			// write "Hash"
			err = en.Append(0xa4, 0x48, 0x61, 0x73, 0x68)
			if err != nil {
				return
			}
			err = en.WriteUint64(za0004[za0005].Hash)
			if err != nil {
				err = msgp.WrapError(err, "KeyMaps", za0003, za0005, "Hash")
				return
			}
			// write "SubstreamId"
			err = en.Append(0xab, 0x53, 0x75, 0x62, 0x73, 0x74, 0x72, 0x65, 0x61, 0x6d, 0x49, 0x64)
			if err != nil {
				return
			}
			err = en.WriteUint8(za0004[za0005].SubstreamId)
			if err != nil {
				err = msgp.WrapError(err, "KeyMaps", za0003, za0005, "SubstreamId")
				return
			}
		}
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
	return
}

// MarshalMsg implements msgp.Marshaler
func (z *ControlMetadata) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	// map header, size 5
	// string "Config"
	o = append(o, 0x85, 0xa6, 0x43, 0x6f, 0x6e, 0x66, 0x69, 0x67)
	o = msgp.AppendMapHeader(o, uint32(len(z.Config)))
	for za0001, za0002 := range z.Config {
		o = msgp.AppendString(o, za0001)
		o = msgp.AppendUint8(o, za0002)
	}
	// string "FinishedPrevTask"
	o = append(o, 0xb0, 0x46, 0x69, 0x6e, 0x69, 0x73, 0x68, 0x65, 0x64, 0x50, 0x72, 0x65, 0x76, 0x54, 0x61, 0x73, 0x6b)
	o = msgp.AppendString(o, z.FinishedPrevTask)
	// string "KeyMaps"
	o = append(o, 0xa7, 0x4b, 0x65, 0x79, 0x4d, 0x61, 0x70, 0x73)
	o = msgp.AppendMapHeader(o, uint32(len(z.KeyMaps)))
	for za0003, za0004 := range z.KeyMaps {
		o = msgp.AppendString(o, za0003)
		o = msgp.AppendArrayHeader(o, uint32(len(za0004)))
		for za0005 := range za0004 {
			// map header, size 3
			// string "Key"
			o = append(o, 0x83, 0xa3, 0x4b, 0x65, 0x79)
			o = msgp.AppendBytes(o, za0004[za0005].Key)
			// string "Hash"
			o = append(o, 0xa4, 0x48, 0x61, 0x73, 0x68)
			o = msgp.AppendUint64(o, za0004[za0005].Hash)
			// string "SubstreamId"
			o = append(o, 0xab, 0x53, 0x75, 0x62, 0x73, 0x74, 0x72, 0x65, 0x61, 0x6d, 0x49, 0x64)
			o = msgp.AppendUint8(o, za0004[za0005].SubstreamId)
		}
	}
	// string "Epoch"
	o = append(o, 0xa5, 0x45, 0x70, 0x6f, 0x63, 0x68)
	o = msgp.AppendUint16(o, z.Epoch)
	// string "InstanceId"
	o = append(o, 0xaa, 0x49, 0x6e, 0x73, 0x74, 0x61, 0x6e, 0x63, 0x65, 0x49, 0x64)
	o = msgp.AppendUint8(o, z.InstanceId)
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
		case "FinishedPrevTask":
			z.FinishedPrevTask, bts, err = msgp.ReadStringBytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "FinishedPrevTask")
				return
			}
		case "KeyMaps":
			var zb0003 uint32
			zb0003, bts, err = msgp.ReadMapHeaderBytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "KeyMaps")
				return
			}
			if z.KeyMaps == nil {
				z.KeyMaps = make(map[string][]KeyMaping, zb0003)
			} else if len(z.KeyMaps) > 0 {
				for key := range z.KeyMaps {
					delete(z.KeyMaps, key)
				}
			}
			for zb0003 > 0 {
				var za0003 string
				var za0004 []KeyMaping
				zb0003--
				za0003, bts, err = msgp.ReadStringBytes(bts)
				if err != nil {
					err = msgp.WrapError(err, "KeyMaps")
					return
				}
				var zb0004 uint32
				zb0004, bts, err = msgp.ReadArrayHeaderBytes(bts)
				if err != nil {
					err = msgp.WrapError(err, "KeyMaps", za0003)
					return
				}
				if cap(za0004) >= int(zb0004) {
					za0004 = (za0004)[:zb0004]
				} else {
					za0004 = make([]KeyMaping, zb0004)
				}
				for za0005 := range za0004 {
					var zb0005 uint32
					zb0005, bts, err = msgp.ReadMapHeaderBytes(bts)
					if err != nil {
						err = msgp.WrapError(err, "KeyMaps", za0003, za0005)
						return
					}
					for zb0005 > 0 {
						zb0005--
						field, bts, err = msgp.ReadMapKeyZC(bts)
						if err != nil {
							err = msgp.WrapError(err, "KeyMaps", za0003, za0005)
							return
						}
						switch msgp.UnsafeString(field) {
						case "Key":
							za0004[za0005].Key, bts, err = msgp.ReadBytesBytes(bts, za0004[za0005].Key)
							if err != nil {
								err = msgp.WrapError(err, "KeyMaps", za0003, za0005, "Key")
								return
							}
						case "Hash":
							za0004[za0005].Hash, bts, err = msgp.ReadUint64Bytes(bts)
							if err != nil {
								err = msgp.WrapError(err, "KeyMaps", za0003, za0005, "Hash")
								return
							}
						case "SubstreamId":
							za0004[za0005].SubstreamId, bts, err = msgp.ReadUint8Bytes(bts)
							if err != nil {
								err = msgp.WrapError(err, "KeyMaps", za0003, za0005, "SubstreamId")
								return
							}
						default:
							bts, err = msgp.Skip(bts)
							if err != nil {
								err = msgp.WrapError(err, "KeyMaps", za0003, za0005)
								return
							}
						}
					}
				}
				z.KeyMaps[za0003] = za0004
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
	s += 17 + msgp.StringPrefixSize + len(z.FinishedPrevTask) + 8 + msgp.MapHeaderSize
	if z.KeyMaps != nil {
		for za0003, za0004 := range z.KeyMaps {
			_ = za0004
			s += msgp.StringPrefixSize + len(za0003) + msgp.ArrayHeaderSize
			for za0005 := range za0004 {
				s += 1 + 4 + msgp.BytesPrefixSize + len(za0004[za0005].Key) + 5 + msgp.Uint64Size + 12 + msgp.Uint8Size
			}
		}
	}
	s += 6 + msgp.Uint16Size + 11 + msgp.Uint8Size
	return
}

// DecodeMsg implements msgp.Decodable
func (z *KeyMaping) DecodeMsg(dc *msgp.Reader) (err error) {
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
func (z *KeyMaping) EncodeMsg(en *msgp.Writer) (err error) {
	// map header, size 3
	// write "Key"
	err = en.Append(0x83, 0xa3, 0x4b, 0x65, 0x79)
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
func (z *KeyMaping) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	// map header, size 3
	// string "Key"
	o = append(o, 0x83, 0xa3, 0x4b, 0x65, 0x79)
	o = msgp.AppendBytes(o, z.Key)
	// string "Hash"
	o = append(o, 0xa4, 0x48, 0x61, 0x73, 0x68)
	o = msgp.AppendUint64(o, z.Hash)
	// string "SubstreamId"
	o = append(o, 0xab, 0x53, 0x75, 0x62, 0x73, 0x74, 0x72, 0x65, 0x61, 0x6d, 0x49, 0x64)
	o = msgp.AppendUint8(o, z.SubstreamId)
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *KeyMaping) UnmarshalMsg(bts []byte) (o []byte, err error) {
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
func (z *KeyMaping) Msgsize() (s int) {
	s = 1 + 4 + msgp.BytesPrefixSize + len(z.Key) + 5 + msgp.Uint64Size + 12 + msgp.Uint8Size
	return
}
