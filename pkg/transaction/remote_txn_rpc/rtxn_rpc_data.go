//go:generate msgp
package remote_txn_rpc

import (
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
	RpcType     RTxnRpcType          `json:"rpcType" msg:"rpcType"`
	SerdeFormat uint8                `json:"serdeFormat" msg:"serdeFormat"`
	Init        *InitArg             `json:"initArg,omitempty" msg:"initArg,omitempty"`
	MetaMsg     *txn_data.TxnMetaMsg `json:"txnMeta,omitempty" msg:"txnMeta,omitempty"`
	ConsumedOff *ConsumedOffsets     `json:"cOff,omitempty" msg:"cOff,omitempty"`
}

type RTxnReply struct {
	Success     bool         `json:"succ,omitempty" msg:"succ,omitempty"`
	Message     string       `json:"msg,omitempty" msg:"msg,omitempty"`
	InitReply   *InitReply   `json:"initRply,omitempty" msg:"initRply,omitempty"`
	CommitReply *CommitReply `json:"cmtRply,omitempty" msg:"cmtRply,omitempty"`
}
