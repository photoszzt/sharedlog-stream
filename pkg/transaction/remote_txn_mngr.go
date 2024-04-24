package transaction

import (
	"context"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/transaction/remote_txn_rpc"
	"sync"

	"cs.utexas.edu/zjia/faas/types"
)

type RemoteTxnManager struct {
	remote_txn_rpc.UnimplementedRemoteTxnMngrServer

	env         types.Environment
	serdeFormat commtypes.SerdeFormat
	mu          sync.Mutex // guard tm_map
	tm_map      map[string]*TransactionManager
	prod_id_map map[string]commtypes.ProducerId
}

func NewRemoteTxnManager(env types.Environment, serdeFormat commtypes.SerdeFormat) (*RemoteTxnManager, error) {
	tm := &RemoteTxnManager{
		env:         env,
		serdeFormat: serdeFormat,
	}
	return tm, nil
}

func (s *RemoteTxnManager) Init(ctx context.Context, in *remote_txn_rpc.InitArg) (*remote_txn_rpc.InitReply, error) {
	tm, err := NewTransactionManager(ctx, s.env, in.TransactionalId, s.serdeFormat)
	if err != nil {
		return nil, err
	}
	initRet, err := tm.InitTransaction(ctx)
	if err != nil {
		return nil, err
	}
	var offsetPairs []*remote_txn_rpc.OffsetPair
	for _, inputTopicInfo := range in.InputTopicInfos {
		inputTopicName := inputTopicInfo.GetTopicName()
		err := tm.CreateOffsetTopic(inputTopicName,
			uint8(inputTopicInfo.GetNumPartition()), inputTopicInfo.GetBufMaxSize())
		if err != nil {
			return nil, err
		}
		if initRet.HasRecentTxnMeta {
			offset, err := GetOffset(ctx, tm, inputTopicInfo.GetTopicName(),
				uint8(in.GetSubstreamNum()))
			if err != nil {
				return nil, err
			}
			offsetPairs = append(offsetPairs, &remote_txn_rpc.OffsetPair{
				TopicName: inputTopicName,
				Offset:    offset,
			})
		}
	}
	s.mu.Lock()
	s.tm_map[in.TransactionalId] = tm
	s.prod_id_map[in.TransactionalId] = tm.prodId
	s.mu.Unlock()
	return &remote_txn_rpc.InitReply{
		ProdId: &commtypes.ProdId{
			TaskId:    tm.prodId.TaskId,
			TaskEpoch: uint32(tm.prodId.TaskEpoch),
		},
		OffsetPairs: offsetPairs,
	}, nil
}
