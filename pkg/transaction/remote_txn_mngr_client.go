package transaction

import (
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/exactly_once_intr"
	"sharedlog-stream/pkg/transaction/remote_txn_rpc"

	"google.golang.org/grpc"
)

type RemoteTxnManagerClient struct {
	remote_txn_rpc.RemoteTxnMngrClient

	prodId commtypes.ProducerId
}

func NewRemoteTxnManagerClient(cc grpc.ClientConnInterface) RemoteTxnManagerClient {
	return RemoteTxnManagerClient{
		RemoteTxnMngrClient: remote_txn_rpc.NewRemoteTxnMngrClient(cc),
	}
}

var _ = exactly_once_intr.ReadOnlyExactlyOnceManager(&RemoteTxnManagerClient{})

func (tm *RemoteTxnManagerClient) GetCurrentEpoch() uint32             { return tm.prodId.TaskEpoch }
func (tm *RemoteTxnManagerClient) GetCurrentTaskId() uint64            { return tm.prodId.TaskId }
func (tm *RemoteTxnManagerClient) GetProducerId() commtypes.ProducerId { return tm.prodId }
func (tm *RemoteTxnManagerClient) UpdateProducerId(prodId *commtypes.ProdId) {
	tm.prodId.TaskId = prodId.TaskId
	tm.prodId.TaskEpoch = prodId.TaskEpoch
}
