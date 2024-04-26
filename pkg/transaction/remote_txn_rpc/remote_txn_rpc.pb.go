// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.28.1
// 	protoc        v5.26.1
// source: pkg/transaction/remote_txn_rpc/remote_txn_rpc.proto

package remote_txn_rpc

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
	reflect "reflect"
	commtypes "sharedlog-stream/pkg/commtypes"
	txn_data "sharedlog-stream/pkg/txn_data"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type StreamInfo struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	TopicName    string `protobuf:"bytes,1,opt,name=topicName,proto3" json:"topicName,omitempty"`
	NumPartition uint32 `protobuf:"varint,2,opt,name=numPartition,proto3" json:"numPartition,omitempty"`
}

func (x *StreamInfo) Reset() {
	*x = StreamInfo{}
	if protoimpl.UnsafeEnabled {
		mi := &file_pkg_transaction_remote_txn_rpc_remote_txn_rpc_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *StreamInfo) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*StreamInfo) ProtoMessage() {}

func (x *StreamInfo) ProtoReflect() protoreflect.Message {
	mi := &file_pkg_transaction_remote_txn_rpc_remote_txn_rpc_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use StreamInfo.ProtoReflect.Descriptor instead.
func (*StreamInfo) Descriptor() ([]byte, []int) {
	return file_pkg_transaction_remote_txn_rpc_remote_txn_rpc_proto_rawDescGZIP(), []int{0}
}

func (x *StreamInfo) GetTopicName() string {
	if x != nil {
		return x.TopicName
	}
	return ""
}

func (x *StreamInfo) GetNumPartition() uint32 {
	if x != nil {
		return x.NumPartition
	}
	return 0
}

type InitArg struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	TransactionalId   string        `protobuf:"bytes,1,opt,name=TransactionalId,proto3" json:"TransactionalId,omitempty"`
	SubstreamNum      uint32        `protobuf:"varint,2,opt,name=SubstreamNum,proto3" json:"SubstreamNum,omitempty"` // which substream this task processes
	BufMaxSize        uint32        `protobuf:"varint,3,opt,name=BufMaxSize,proto3" json:"BufMaxSize,omitempty"`
	InputStreamInfos  []*StreamInfo `protobuf:"bytes,4,rep,name=InputStreamInfos,proto3" json:"InputStreamInfos,omitempty"`
	OutputStreamInfos []*StreamInfo `protobuf:"bytes,5,rep,name=OutputStreamInfos,proto3" json:"OutputStreamInfos,omitempty"`
	KVChangelogInfos  []*StreamInfo `protobuf:"bytes,6,rep,name=KVChangelogInfos,proto3" json:"KVChangelogInfos,omitempty"`
	WinChangelogInfos []*StreamInfo `protobuf:"bytes,7,rep,name=WinChangelogInfos,proto3" json:"WinChangelogInfos,omitempty"`
}

func (x *InitArg) Reset() {
	*x = InitArg{}
	if protoimpl.UnsafeEnabled {
		mi := &file_pkg_transaction_remote_txn_rpc_remote_txn_rpc_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *InitArg) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*InitArg) ProtoMessage() {}

func (x *InitArg) ProtoReflect() protoreflect.Message {
	mi := &file_pkg_transaction_remote_txn_rpc_remote_txn_rpc_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use InitArg.ProtoReflect.Descriptor instead.
func (*InitArg) Descriptor() ([]byte, []int) {
	return file_pkg_transaction_remote_txn_rpc_remote_txn_rpc_proto_rawDescGZIP(), []int{1}
}

func (x *InitArg) GetTransactionalId() string {
	if x != nil {
		return x.TransactionalId
	}
	return ""
}

func (x *InitArg) GetSubstreamNum() uint32 {
	if x != nil {
		return x.SubstreamNum
	}
	return 0
}

func (x *InitArg) GetBufMaxSize() uint32 {
	if x != nil {
		return x.BufMaxSize
	}
	return 0
}

func (x *InitArg) GetInputStreamInfos() []*StreamInfo {
	if x != nil {
		return x.InputStreamInfos
	}
	return nil
}

func (x *InitArg) GetOutputStreamInfos() []*StreamInfo {
	if x != nil {
		return x.OutputStreamInfos
	}
	return nil
}

func (x *InitArg) GetKVChangelogInfos() []*StreamInfo {
	if x != nil {
		return x.KVChangelogInfos
	}
	return nil
}

func (x *InitArg) GetWinChangelogInfos() []*StreamInfo {
	if x != nil {
		return x.WinChangelogInfos
	}
	return nil
}

type InitReply struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	ProdId      *commtypes.ProdId `protobuf:"bytes,1,opt,name=prodId,proto3" json:"prodId,omitempty"`
	OffsetPairs []*OffsetPair     `protobuf:"bytes,2,rep,name=offsetPairs,proto3" json:"offsetPairs,omitempty"`
}

func (x *InitReply) Reset() {
	*x = InitReply{}
	if protoimpl.UnsafeEnabled {
		mi := &file_pkg_transaction_remote_txn_rpc_remote_txn_rpc_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *InitReply) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*InitReply) ProtoMessage() {}

func (x *InitReply) ProtoReflect() protoreflect.Message {
	mi := &file_pkg_transaction_remote_txn_rpc_remote_txn_rpc_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use InitReply.ProtoReflect.Descriptor instead.
func (*InitReply) Descriptor() ([]byte, []int) {
	return file_pkg_transaction_remote_txn_rpc_remote_txn_rpc_proto_rawDescGZIP(), []int{2}
}

func (x *InitReply) GetProdId() *commtypes.ProdId {
	if x != nil {
		return x.ProdId
	}
	return nil
}

func (x *InitReply) GetOffsetPairs() []*OffsetPair {
	if x != nil {
		return x.OffsetPairs
	}
	return nil
}

type OffsetPair struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	TopicName string `protobuf:"bytes,1,opt,name=topicName,proto3" json:"topicName,omitempty"`
	Offset    uint64 `protobuf:"varint,2,opt,name=offset,proto3" json:"offset,omitempty"`
}

func (x *OffsetPair) Reset() {
	*x = OffsetPair{}
	if protoimpl.UnsafeEnabled {
		mi := &file_pkg_transaction_remote_txn_rpc_remote_txn_rpc_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *OffsetPair) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*OffsetPair) ProtoMessage() {}

func (x *OffsetPair) ProtoReflect() protoreflect.Message {
	mi := &file_pkg_transaction_remote_txn_rpc_remote_txn_rpc_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use OffsetPair.ProtoReflect.Descriptor instead.
func (*OffsetPair) Descriptor() ([]byte, []int) {
	return file_pkg_transaction_remote_txn_rpc_remote_txn_rpc_proto_rawDescGZIP(), []int{3}
}

func (x *OffsetPair) GetTopicName() string {
	if x != nil {
		return x.TopicName
	}
	return ""
}

func (x *OffsetPair) GetOffset() uint64 {
	if x != nil {
		return x.Offset
	}
	return 0
}

var File_pkg_transaction_remote_txn_rpc_remote_txn_rpc_proto protoreflect.FileDescriptor

var file_pkg_transaction_remote_txn_rpc_remote_txn_rpc_proto_rawDesc = []byte{
	0x0a, 0x33, 0x70, 0x6b, 0x67, 0x2f, 0x74, 0x72, 0x61, 0x6e, 0x73, 0x61, 0x63, 0x74, 0x69, 0x6f,
	0x6e, 0x2f, 0x72, 0x65, 0x6d, 0x6f, 0x74, 0x65, 0x5f, 0x74, 0x78, 0x6e, 0x5f, 0x72, 0x70, 0x63,
	0x2f, 0x72, 0x65, 0x6d, 0x6f, 0x74, 0x65, 0x5f, 0x74, 0x78, 0x6e, 0x5f, 0x72, 0x70, 0x63, 0x2e,
	0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x0e, 0x72, 0x65, 0x6d, 0x6f, 0x74, 0x65, 0x5f, 0x74, 0x78,
	0x6e, 0x5f, 0x72, 0x70, 0x63, 0x1a, 0x22, 0x70, 0x6b, 0x67, 0x2f, 0x63, 0x6f, 0x6d, 0x6d, 0x74,
	0x79, 0x70, 0x65, 0x73, 0x2f, 0x70, 0x72, 0x6f, 0x64, 0x75, 0x63, 0x65, 0x72, 0x5f, 0x73, 0x74,
	0x61, 0x74, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x1a, 0x1b, 0x70, 0x6b, 0x67, 0x2f, 0x74,
	0x78, 0x6e, 0x5f, 0x64, 0x61, 0x74, 0x61, 0x2f, 0x74, 0x78, 0x6e, 0x5f, 0x6d, 0x65, 0x74, 0x61,
	0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x1a, 0x1b, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2f, 0x70,
	0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2f, 0x65, 0x6d, 0x70, 0x74, 0x79, 0x2e, 0x70, 0x72,
	0x6f, 0x74, 0x6f, 0x22, 0x4e, 0x0a, 0x0a, 0x53, 0x74, 0x72, 0x65, 0x61, 0x6d, 0x49, 0x6e, 0x66,
	0x6f, 0x12, 0x1c, 0x0a, 0x09, 0x74, 0x6f, 0x70, 0x69, 0x63, 0x4e, 0x61, 0x6d, 0x65, 0x18, 0x01,
	0x20, 0x01, 0x28, 0x09, 0x52, 0x09, 0x74, 0x6f, 0x70, 0x69, 0x63, 0x4e, 0x61, 0x6d, 0x65, 0x12,
	0x22, 0x0a, 0x0c, 0x6e, 0x75, 0x6d, 0x50, 0x61, 0x72, 0x74, 0x69, 0x74, 0x69, 0x6f, 0x6e, 0x18,
	0x02, 0x20, 0x01, 0x28, 0x0d, 0x52, 0x0c, 0x6e, 0x75, 0x6d, 0x50, 0x61, 0x72, 0x74, 0x69, 0x74,
	0x69, 0x6f, 0x6e, 0x22, 0x9b, 0x03, 0x0a, 0x07, 0x49, 0x6e, 0x69, 0x74, 0x41, 0x72, 0x67, 0x12,
	0x28, 0x0a, 0x0f, 0x54, 0x72, 0x61, 0x6e, 0x73, 0x61, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x61, 0x6c,
	0x49, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0f, 0x54, 0x72, 0x61, 0x6e, 0x73, 0x61,
	0x63, 0x74, 0x69, 0x6f, 0x6e, 0x61, 0x6c, 0x49, 0x64, 0x12, 0x22, 0x0a, 0x0c, 0x53, 0x75, 0x62,
	0x73, 0x74, 0x72, 0x65, 0x61, 0x6d, 0x4e, 0x75, 0x6d, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0d, 0x52,
	0x0c, 0x53, 0x75, 0x62, 0x73, 0x74, 0x72, 0x65, 0x61, 0x6d, 0x4e, 0x75, 0x6d, 0x12, 0x1e, 0x0a,
	0x0a, 0x42, 0x75, 0x66, 0x4d, 0x61, 0x78, 0x53, 0x69, 0x7a, 0x65, 0x18, 0x03, 0x20, 0x01, 0x28,
	0x0d, 0x52, 0x0a, 0x42, 0x75, 0x66, 0x4d, 0x61, 0x78, 0x53, 0x69, 0x7a, 0x65, 0x12, 0x46, 0x0a,
	0x10, 0x49, 0x6e, 0x70, 0x75, 0x74, 0x53, 0x74, 0x72, 0x65, 0x61, 0x6d, 0x49, 0x6e, 0x66, 0x6f,
	0x73, 0x18, 0x04, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x1a, 0x2e, 0x72, 0x65, 0x6d, 0x6f, 0x74, 0x65,
	0x5f, 0x74, 0x78, 0x6e, 0x5f, 0x72, 0x70, 0x63, 0x2e, 0x53, 0x74, 0x72, 0x65, 0x61, 0x6d, 0x49,
	0x6e, 0x66, 0x6f, 0x52, 0x10, 0x49, 0x6e, 0x70, 0x75, 0x74, 0x53, 0x74, 0x72, 0x65, 0x61, 0x6d,
	0x49, 0x6e, 0x66, 0x6f, 0x73, 0x12, 0x48, 0x0a, 0x11, 0x4f, 0x75, 0x74, 0x70, 0x75, 0x74, 0x53,
	0x74, 0x72, 0x65, 0x61, 0x6d, 0x49, 0x6e, 0x66, 0x6f, 0x73, 0x18, 0x05, 0x20, 0x03, 0x28, 0x0b,
	0x32, 0x1a, 0x2e, 0x72, 0x65, 0x6d, 0x6f, 0x74, 0x65, 0x5f, 0x74, 0x78, 0x6e, 0x5f, 0x72, 0x70,
	0x63, 0x2e, 0x53, 0x74, 0x72, 0x65, 0x61, 0x6d, 0x49, 0x6e, 0x66, 0x6f, 0x52, 0x11, 0x4f, 0x75,
	0x74, 0x70, 0x75, 0x74, 0x53, 0x74, 0x72, 0x65, 0x61, 0x6d, 0x49, 0x6e, 0x66, 0x6f, 0x73, 0x12,
	0x46, 0x0a, 0x10, 0x4b, 0x56, 0x43, 0x68, 0x61, 0x6e, 0x67, 0x65, 0x6c, 0x6f, 0x67, 0x49, 0x6e,
	0x66, 0x6f, 0x73, 0x18, 0x06, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x1a, 0x2e, 0x72, 0x65, 0x6d, 0x6f,
	0x74, 0x65, 0x5f, 0x74, 0x78, 0x6e, 0x5f, 0x72, 0x70, 0x63, 0x2e, 0x53, 0x74, 0x72, 0x65, 0x61,
	0x6d, 0x49, 0x6e, 0x66, 0x6f, 0x52, 0x10, 0x4b, 0x56, 0x43, 0x68, 0x61, 0x6e, 0x67, 0x65, 0x6c,
	0x6f, 0x67, 0x49, 0x6e, 0x66, 0x6f, 0x73, 0x12, 0x48, 0x0a, 0x11, 0x57, 0x69, 0x6e, 0x43, 0x68,
	0x61, 0x6e, 0x67, 0x65, 0x6c, 0x6f, 0x67, 0x49, 0x6e, 0x66, 0x6f, 0x73, 0x18, 0x07, 0x20, 0x03,
	0x28, 0x0b, 0x32, 0x1a, 0x2e, 0x72, 0x65, 0x6d, 0x6f, 0x74, 0x65, 0x5f, 0x74, 0x78, 0x6e, 0x5f,
	0x72, 0x70, 0x63, 0x2e, 0x53, 0x74, 0x72, 0x65, 0x61, 0x6d, 0x49, 0x6e, 0x66, 0x6f, 0x52, 0x11,
	0x57, 0x69, 0x6e, 0x43, 0x68, 0x61, 0x6e, 0x67, 0x65, 0x6c, 0x6f, 0x67, 0x49, 0x6e, 0x66, 0x6f,
	0x73, 0x22, 0x74, 0x0a, 0x09, 0x49, 0x6e, 0x69, 0x74, 0x52, 0x65, 0x70, 0x6c, 0x79, 0x12, 0x29,
	0x0a, 0x06, 0x70, 0x72, 0x6f, 0x64, 0x49, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x11,
	0x2e, 0x63, 0x6f, 0x6d, 0x6d, 0x74, 0x79, 0x70, 0x65, 0x73, 0x2e, 0x50, 0x72, 0x6f, 0x64, 0x49,
	0x64, 0x52, 0x06, 0x70, 0x72, 0x6f, 0x64, 0x49, 0x64, 0x12, 0x3c, 0x0a, 0x0b, 0x6f, 0x66, 0x66,
	0x73, 0x65, 0x74, 0x50, 0x61, 0x69, 0x72, 0x73, 0x18, 0x02, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x1a,
	0x2e, 0x72, 0x65, 0x6d, 0x6f, 0x74, 0x65, 0x5f, 0x74, 0x78, 0x6e, 0x5f, 0x72, 0x70, 0x63, 0x2e,
	0x4f, 0x66, 0x66, 0x73, 0x65, 0x74, 0x50, 0x61, 0x69, 0x72, 0x52, 0x0b, 0x6f, 0x66, 0x66, 0x73,
	0x65, 0x74, 0x50, 0x61, 0x69, 0x72, 0x73, 0x22, 0x42, 0x0a, 0x0a, 0x4f, 0x66, 0x66, 0x73, 0x65,
	0x74, 0x50, 0x61, 0x69, 0x72, 0x12, 0x1c, 0x0a, 0x09, 0x74, 0x6f, 0x70, 0x69, 0x63, 0x4e, 0x61,
	0x6d, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x09, 0x74, 0x6f, 0x70, 0x69, 0x63, 0x4e,
	0x61, 0x6d, 0x65, 0x12, 0x16, 0x0a, 0x06, 0x6f, 0x66, 0x66, 0x73, 0x65, 0x74, 0x18, 0x02, 0x20,
	0x01, 0x28, 0x04, 0x52, 0x06, 0x6f, 0x66, 0x66, 0x73, 0x65, 0x74, 0x32, 0x92, 0x02, 0x0a, 0x0d,
	0x52, 0x65, 0x6d, 0x6f, 0x74, 0x65, 0x54, 0x78, 0x6e, 0x4d, 0x6e, 0x67, 0x72, 0x12, 0x3c, 0x0a,
	0x04, 0x49, 0x6e, 0x69, 0x74, 0x12, 0x17, 0x2e, 0x72, 0x65, 0x6d, 0x6f, 0x74, 0x65, 0x5f, 0x74,
	0x78, 0x6e, 0x5f, 0x72, 0x70, 0x63, 0x2e, 0x49, 0x6e, 0x69, 0x74, 0x41, 0x72, 0x67, 0x1a, 0x19,
	0x2e, 0x72, 0x65, 0x6d, 0x6f, 0x74, 0x65, 0x5f, 0x74, 0x78, 0x6e, 0x5f, 0x72, 0x70, 0x63, 0x2e,
	0x49, 0x6e, 0x69, 0x74, 0x52, 0x65, 0x70, 0x6c, 0x79, 0x22, 0x00, 0x12, 0x3d, 0x0a, 0x0b, 0x41,
	0x70, 0x70, 0x65, 0x6e, 0x64, 0x54, 0x70, 0x50, 0x61, 0x72, 0x12, 0x14, 0x2e, 0x74, 0x78, 0x6e,
	0x5f, 0x64, 0x61, 0x74, 0x61, 0x2e, 0x54, 0x78, 0x6e, 0x4d, 0x65, 0x74, 0x61, 0x4d, 0x73, 0x67,
	0x1a, 0x16, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62,
	0x75, 0x66, 0x2e, 0x45, 0x6d, 0x70, 0x74, 0x79, 0x22, 0x00, 0x12, 0x3a, 0x0a, 0x08, 0x41, 0x62,
	0x6f, 0x72, 0x74, 0x54, 0x78, 0x6e, 0x12, 0x14, 0x2e, 0x74, 0x78, 0x6e, 0x5f, 0x64, 0x61, 0x74,
	0x61, 0x2e, 0x54, 0x78, 0x6e, 0x4d, 0x65, 0x74, 0x61, 0x4d, 0x73, 0x67, 0x1a, 0x16, 0x2e, 0x67,
	0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e, 0x45,
	0x6d, 0x70, 0x74, 0x79, 0x22, 0x00, 0x12, 0x48, 0x0a, 0x16, 0x43, 0x6f, 0x6d, 0x6d, 0x69, 0x74,
	0x54, 0x78, 0x6e, 0x41, 0x73, 0x79, 0x6e, 0x63, 0x43, 0x6f, 0x6d, 0x70, 0x6c, 0x65, 0x74, 0x65,
	0x12, 0x14, 0x2e, 0x74, 0x78, 0x6e, 0x5f, 0x64, 0x61, 0x74, 0x61, 0x2e, 0x54, 0x78, 0x6e, 0x4d,
	0x65, 0x74, 0x61, 0x4d, 0x73, 0x67, 0x1a, 0x16, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e,
	0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e, 0x45, 0x6d, 0x70, 0x74, 0x79, 0x22, 0x00,
	0x42, 0x31, 0x5a, 0x2f, 0x73, 0x68, 0x61, 0x72, 0x65, 0x64, 0x6c, 0x6f, 0x67, 0x2d, 0x73, 0x74,
	0x72, 0x65, 0x61, 0x6d, 0x2f, 0x70, 0x6b, 0x67, 0x2f, 0x74, 0x72, 0x61, 0x6e, 0x73, 0x61, 0x63,
	0x74, 0x69, 0x6f, 0x6e, 0x2f, 0x72, 0x65, 0x6d, 0x6f, 0x74, 0x65, 0x5f, 0x74, 0x78, 0x6e, 0x5f,
	0x72, 0x70, 0x63, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_pkg_transaction_remote_txn_rpc_remote_txn_rpc_proto_rawDescOnce sync.Once
	file_pkg_transaction_remote_txn_rpc_remote_txn_rpc_proto_rawDescData = file_pkg_transaction_remote_txn_rpc_remote_txn_rpc_proto_rawDesc
)

func file_pkg_transaction_remote_txn_rpc_remote_txn_rpc_proto_rawDescGZIP() []byte {
	file_pkg_transaction_remote_txn_rpc_remote_txn_rpc_proto_rawDescOnce.Do(func() {
		file_pkg_transaction_remote_txn_rpc_remote_txn_rpc_proto_rawDescData = protoimpl.X.CompressGZIP(file_pkg_transaction_remote_txn_rpc_remote_txn_rpc_proto_rawDescData)
	})
	return file_pkg_transaction_remote_txn_rpc_remote_txn_rpc_proto_rawDescData
}

var file_pkg_transaction_remote_txn_rpc_remote_txn_rpc_proto_msgTypes = make([]protoimpl.MessageInfo, 4)
var file_pkg_transaction_remote_txn_rpc_remote_txn_rpc_proto_goTypes = []interface{}{
	(*StreamInfo)(nil),          // 0: remote_txn_rpc.StreamInfo
	(*InitArg)(nil),             // 1: remote_txn_rpc.InitArg
	(*InitReply)(nil),           // 2: remote_txn_rpc.InitReply
	(*OffsetPair)(nil),          // 3: remote_txn_rpc.OffsetPair
	(*commtypes.ProdId)(nil),    // 4: commtypes.ProdId
	(*txn_data.TxnMetaMsg)(nil), // 5: txn_data.TxnMetaMsg
	(*emptypb.Empty)(nil),       // 6: google.protobuf.Empty
}
var file_pkg_transaction_remote_txn_rpc_remote_txn_rpc_proto_depIdxs = []int32{
	0,  // 0: remote_txn_rpc.InitArg.InputStreamInfos:type_name -> remote_txn_rpc.StreamInfo
	0,  // 1: remote_txn_rpc.InitArg.OutputStreamInfos:type_name -> remote_txn_rpc.StreamInfo
	0,  // 2: remote_txn_rpc.InitArg.KVChangelogInfos:type_name -> remote_txn_rpc.StreamInfo
	0,  // 3: remote_txn_rpc.InitArg.WinChangelogInfos:type_name -> remote_txn_rpc.StreamInfo
	4,  // 4: remote_txn_rpc.InitReply.prodId:type_name -> commtypes.ProdId
	3,  // 5: remote_txn_rpc.InitReply.offsetPairs:type_name -> remote_txn_rpc.OffsetPair
	1,  // 6: remote_txn_rpc.RemoteTxnMngr.Init:input_type -> remote_txn_rpc.InitArg
	5,  // 7: remote_txn_rpc.RemoteTxnMngr.AppendTpPar:input_type -> txn_data.TxnMetaMsg
	5,  // 8: remote_txn_rpc.RemoteTxnMngr.AbortTxn:input_type -> txn_data.TxnMetaMsg
	5,  // 9: remote_txn_rpc.RemoteTxnMngr.CommitTxnAsyncComplete:input_type -> txn_data.TxnMetaMsg
	2,  // 10: remote_txn_rpc.RemoteTxnMngr.Init:output_type -> remote_txn_rpc.InitReply
	6,  // 11: remote_txn_rpc.RemoteTxnMngr.AppendTpPar:output_type -> google.protobuf.Empty
	6,  // 12: remote_txn_rpc.RemoteTxnMngr.AbortTxn:output_type -> google.protobuf.Empty
	6,  // 13: remote_txn_rpc.RemoteTxnMngr.CommitTxnAsyncComplete:output_type -> google.protobuf.Empty
	10, // [10:14] is the sub-list for method output_type
	6,  // [6:10] is the sub-list for method input_type
	6,  // [6:6] is the sub-list for extension type_name
	6,  // [6:6] is the sub-list for extension extendee
	0,  // [0:6] is the sub-list for field type_name
}

func init() { file_pkg_transaction_remote_txn_rpc_remote_txn_rpc_proto_init() }
func file_pkg_transaction_remote_txn_rpc_remote_txn_rpc_proto_init() {
	if File_pkg_transaction_remote_txn_rpc_remote_txn_rpc_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_pkg_transaction_remote_txn_rpc_remote_txn_rpc_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*StreamInfo); i {
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
		file_pkg_transaction_remote_txn_rpc_remote_txn_rpc_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*InitArg); i {
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
		file_pkg_transaction_remote_txn_rpc_remote_txn_rpc_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*InitReply); i {
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
		file_pkg_transaction_remote_txn_rpc_remote_txn_rpc_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*OffsetPair); i {
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
			RawDescriptor: file_pkg_transaction_remote_txn_rpc_remote_txn_rpc_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   4,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_pkg_transaction_remote_txn_rpc_remote_txn_rpc_proto_goTypes,
		DependencyIndexes: file_pkg_transaction_remote_txn_rpc_remote_txn_rpc_proto_depIdxs,
		MessageInfos:      file_pkg_transaction_remote_txn_rpc_remote_txn_rpc_proto_msgTypes,
	}.Build()
	File_pkg_transaction_remote_txn_rpc_remote_txn_rpc_proto = out.File
	file_pkg_transaction_remote_txn_rpc_remote_txn_rpc_proto_rawDesc = nil
	file_pkg_transaction_remote_txn_rpc_remote_txn_rpc_proto_goTypes = nil
	file_pkg_transaction_remote_txn_rpc_remote_txn_rpc_proto_depIdxs = nil
}
