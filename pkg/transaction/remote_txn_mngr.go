package transaction

import (
	"context"
	"sharedlog-stream/pkg/common_errors"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/transaction/remote_txn_rpc"
	"sharedlog-stream/pkg/txn_data"
	"sync"

	"cs.utexas.edu/zjia/faas/types"
	"google.golang.org/protobuf/types/known/emptypb"
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
	for _, inputTopicInfo := range in.InputStreamInfos {
		inputTopicName := inputTopicInfo.GetTopicName()
		stream, err := sharedlog_stream.NewShardedSharedLogStream(s.env, inputTopicName,
			uint8(inputTopicInfo.NumPartition), s.serdeFormat, in.GetBufMaxSize())
		if err != nil {
			return nil, err
		}
		tm.RecordTopicStreams(inputTopicName, stream)
		err = tm.CreateOffsetTopic(inputTopicName,
			uint8(inputTopicInfo.GetNumPartition()), in.GetBufMaxSize())
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
	for _, outStreamInfo := range in.OutputStreamInfos {
		stream, err := sharedlog_stream.NewShardedSharedLogStream(s.env, outStreamInfo.GetTopicName(),
			uint8(outStreamInfo.NumPartition), s.serdeFormat, in.GetBufMaxSize())
		if err != nil {
			return nil, err
		}
		tm.RecordTopicStreams(outStreamInfo.GetTopicName(), stream)
	}
	for _, kvsInfo := range in.KVChangelogInfos {
		stream, err := sharedlog_stream.NewShardedSharedLogStream(s.env, kvsInfo.GetTopicName(),
			uint8(kvsInfo.GetNumPartition()), s.serdeFormat, in.GetBufMaxSize())
		if err != nil {
			return nil, err
		}
		tm.RecordTopicStreams(kvsInfo.GetTopicName(), stream)
	}
	for _, wsInfo := range in.WinChangelogInfos {
		stream, err := sharedlog_stream.NewShardedSharedLogStream(s.env, wsInfo.GetTopicName(),
			uint8(wsInfo.GetNumPartition()), s.serdeFormat, in.GetBufMaxSize())
		if err != nil {
			return nil, err
		}
		tm.RecordTopicStreams(wsInfo.GetTopicName(), stream)
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

func (s *RemoteTxnManager) AppendTpPar(ctx context.Context, in *txn_data.TxnMetaMsg) (*emptypb.Empty, error) {
	s.mu.Lock()
	prodId := s.prod_id_map[in.TransactionalId]
	if prodId.TaskEpoch != in.ProdId.GetTaskEpoch() || prodId.TaskId != in.ProdId.GetTaskId() {
		return nil, common_errors.ErrStaleProducer
	}
	tm := s.tm_map[in.TransactionalId]
	s.mu.Unlock()
	txnMeta := txn_data.TxnMetadata{
		State:           txn_data.TransactionState(in.State),
		TopicPartitions: in.TopicPartitions,
	}
	_, err := tm.appendToTransactionLog(ctx, txnMeta, []uint64{tm.txnLogTag})
	if err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}
