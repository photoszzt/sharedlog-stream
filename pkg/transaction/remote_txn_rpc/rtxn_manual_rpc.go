package remote_txn_rpc

import (
	context "context"
	txn_data "sharedlog-stream/pkg/txn_data"
)

type RemoteTxnClient interface {
	Init(ctx context.Context, in *InitArg) (*InitReply, error)
	AppendTpPar(ctx context.Context, in *txn_data.TxnMetaMsg) error
	AbortTxn(ctx context.Context, in *txn_data.TxnMetaMsg) error
	CommitTxnAsyncComplete(ctx context.Context, in *txn_data.TxnMetaMsg) (*CommitReply, error)
	AppendConsumedOffset(ctx context.Context, in *ConsumedOffsets) error
}
