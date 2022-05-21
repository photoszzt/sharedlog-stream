package commtypes

type KeyT interface{}

type ValueT interface{}

type KVMsgSerdes struct {
	KeySerde Serde
	ValSerde Serde
	MsgSerde MsgSerde
}

type CreateStreamParam struct {
	Format       SerdeFormat
	NumPartition uint8
}
