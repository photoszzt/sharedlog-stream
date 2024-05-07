//go:generate msgp
package remote_txn_rpc

import (
	commtypes "sharedlog-stream/pkg/commtypes"
	txn_data "sharedlog-stream/pkg/txn_data"
)

type RTxnRpcType uint8

const (
	Init RTxnRpcType = iota
	AppendTpPar
	AbortTxn
	CommitTxnAsync
	AppendConsumedOff
)

type RTxnArg struct {
	RpcType     RTxnRpcType           `json:"rpcType" msg:"rpcType"`
	SerdeFormat commtypes.SerdeFormat `json:"serdeFormat" msg:"serdeFormat"`
	Init        *InitArg              `json:"initArg,omitempty" msg:"initArg,omitempty"`
	MetaMsg     *txn_data.TxnMetaMsg  `json:"txnMeta,omitempty" msg:"txnMeta,omitempty"`
	ConsumedOff *ConsumedOffsets      `json:"cOff,omitempty" msg:"cOff,omitempty"`
}
