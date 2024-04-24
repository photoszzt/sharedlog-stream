// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.28.1
// 	protoc        v5.26.1
// source: pkg/commtypes/producer_state.proto

package commtypes

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type ProdId struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	TaskId    uint64 `protobuf:"varint,1,opt,name=task_id,json=taskId,proto3" json:"task_id,omitempty"`
	TaskEpoch uint32 `protobuf:"varint,2,opt,name=task_epoch,json=taskEpoch,proto3" json:"task_epoch,omitempty"`
}

func (x *ProdId) Reset() {
	*x = ProdId{}
	if protoimpl.UnsafeEnabled {
		mi := &file_pkg_commtypes_producer_state_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ProdId) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ProdId) ProtoMessage() {}

func (x *ProdId) ProtoReflect() protoreflect.Message {
	mi := &file_pkg_commtypes_producer_state_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ProdId.ProtoReflect.Descriptor instead.
func (*ProdId) Descriptor() ([]byte, []int) {
	return file_pkg_commtypes_producer_state_proto_rawDescGZIP(), []int{0}
}

func (x *ProdId) GetTaskId() uint64 {
	if x != nil {
		return x.TaskId
	}
	return 0
}

func (x *ProdId) GetTaskEpoch() uint32 {
	if x != nil {
		return x.TaskEpoch
	}
	return 0
}

var File_pkg_commtypes_producer_state_proto protoreflect.FileDescriptor

var file_pkg_commtypes_producer_state_proto_rawDesc = []byte{
	0x0a, 0x22, 0x70, 0x6b, 0x67, 0x2f, 0x63, 0x6f, 0x6d, 0x6d, 0x74, 0x79, 0x70, 0x65, 0x73, 0x2f,
	0x70, 0x72, 0x6f, 0x64, 0x75, 0x63, 0x65, 0x72, 0x5f, 0x73, 0x74, 0x61, 0x74, 0x65, 0x2e, 0x70,
	0x72, 0x6f, 0x74, 0x6f, 0x12, 0x0e, 0x70, 0x72, 0x6f, 0x64, 0x75, 0x63, 0x65, 0x72, 0x5f, 0x73,
	0x74, 0x61, 0x74, 0x65, 0x22, 0x40, 0x0a, 0x06, 0x50, 0x72, 0x6f, 0x64, 0x49, 0x64, 0x12, 0x17,
	0x0a, 0x07, 0x74, 0x61, 0x73, 0x6b, 0x5f, 0x69, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x04, 0x52,
	0x06, 0x74, 0x61, 0x73, 0x6b, 0x49, 0x64, 0x12, 0x1d, 0x0a, 0x0a, 0x74, 0x61, 0x73, 0x6b, 0x5f,
	0x65, 0x70, 0x6f, 0x63, 0x68, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0d, 0x52, 0x09, 0x74, 0x61, 0x73,
	0x6b, 0x45, 0x70, 0x6f, 0x63, 0x68, 0x42, 0x20, 0x5a, 0x1e, 0x73, 0x68, 0x61, 0x72, 0x65, 0x64,
	0x6c, 0x6f, 0x67, 0x2d, 0x73, 0x74, 0x72, 0x65, 0x61, 0x6d, 0x2f, 0x70, 0x6b, 0x67, 0x2f, 0x63,
	0x6f, 0x6d, 0x6d, 0x74, 0x79, 0x70, 0x65, 0x73, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_pkg_commtypes_producer_state_proto_rawDescOnce sync.Once
	file_pkg_commtypes_producer_state_proto_rawDescData = file_pkg_commtypes_producer_state_proto_rawDesc
)

func file_pkg_commtypes_producer_state_proto_rawDescGZIP() []byte {
	file_pkg_commtypes_producer_state_proto_rawDescOnce.Do(func() {
		file_pkg_commtypes_producer_state_proto_rawDescData = protoimpl.X.CompressGZIP(file_pkg_commtypes_producer_state_proto_rawDescData)
	})
	return file_pkg_commtypes_producer_state_proto_rawDescData
}

var file_pkg_commtypes_producer_state_proto_msgTypes = make([]protoimpl.MessageInfo, 1)
var file_pkg_commtypes_producer_state_proto_goTypes = []interface{}{
	(*ProdId)(nil), // 0: producer_state.ProdId
}
var file_pkg_commtypes_producer_state_proto_depIdxs = []int32{
	0, // [0:0] is the sub-list for method output_type
	0, // [0:0] is the sub-list for method input_type
	0, // [0:0] is the sub-list for extension type_name
	0, // [0:0] is the sub-list for extension extendee
	0, // [0:0] is the sub-list for field type_name
}

func init() { file_pkg_commtypes_producer_state_proto_init() }
func file_pkg_commtypes_producer_state_proto_init() {
	if File_pkg_commtypes_producer_state_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_pkg_commtypes_producer_state_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ProdId); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_pkg_commtypes_producer_state_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   1,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_pkg_commtypes_producer_state_proto_goTypes,
		DependencyIndexes: file_pkg_commtypes_producer_state_proto_depIdxs,
		MessageInfos:      file_pkg_commtypes_producer_state_proto_msgTypes,
	}.Build()
	File_pkg_commtypes_producer_state_proto = out.File
	file_pkg_commtypes_producer_state_proto_rawDesc = nil
	file_pkg_commtypes_producer_state_proto_goTypes = nil
	file_pkg_commtypes_producer_state_proto_depIdxs = nil
}
