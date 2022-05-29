package commtypes

import "cs.utexas.edu/zjia/faas/types"

type KeyT interface{}

type ValueT interface{}

type KVMsgSerdes struct {
	KeySerde Serde
	ValSerde Serde
	MsgSerde MsgSerde
}

type CreateStreamParam struct {
	Env          types.Environment
	NumPartition uint8
}
