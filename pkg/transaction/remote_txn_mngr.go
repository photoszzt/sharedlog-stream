package transaction

import (
	"context"
	"os"
	"sharedlog-stream/pkg/common_errors"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/debug"
	"sharedlog-stream/pkg/env_config"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/stats"
	"sharedlog-stream/pkg/transaction/remote_txn_rpc"
	"sharedlog-stream/pkg/txn_data"
	"sync"

	"cs.utexas.edu/zjia/faas/types"
	"golang.org/x/sync/errgroup"
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

func NewRemoteTxnManager(env types.Environment, serdeFormat commtypes.SerdeFormat) *RemoteTxnManager {
	tm := &RemoteTxnManager{
		env:         env,
		serdeFormat: serdeFormat,
		tm_map:      make(map[string]*TransactionManager),
		prod_id_map: make(map[string]commtypes.ProducerId),
	}
	return tm
}

func (s *RemoteTxnManager) Init(ctx context.Context, in *remote_txn_rpc.InitArg) (*remote_txn_rpc.InitReply, error) {
	debug.Fprintf(os.Stderr, "handle Init with input: %v\n", in)
	tm, err := NewTransactionManager(ctx, s.env, in.TransactionalId, s.serdeFormat)
	if err != nil {
		return nil, err
	}
	debug.Fprint(os.Stderr, "init 1\n")
	initRet, err := tm.InitTransaction(ctx)
	if err != nil {
		return nil, err
	}
	debug.Fprint(os.Stderr, "init 2\n")
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
	debug.Fprint(os.Stderr, "init 3\n")
	for _, outStreamInfo := range in.OutputStreamInfos {
		stream, err := sharedlog_stream.NewShardedSharedLogStream(s.env, outStreamInfo.GetTopicName(),
			uint8(outStreamInfo.NumPartition), s.serdeFormat, in.GetBufMaxSize())
		if err != nil {
			return nil, err
		}
		tm.RecordTopicStreams(outStreamInfo.GetTopicName(), stream)
	}
	debug.Fprint(os.Stderr, "init 4\n")
	for _, kvsInfo := range in.KVChangelogInfos {
		stream, err := sharedlog_stream.NewShardedSharedLogStream(s.env, kvsInfo.GetTopicName(),
			uint8(kvsInfo.GetNumPartition()), s.serdeFormat, in.GetBufMaxSize())
		if err != nil {
			return nil, err
		}
		tm.RecordTopicStreams(kvsInfo.GetTopicName(), stream)
	}
	debug.Fprint(os.Stderr, "init 5\n")
	for _, wsInfo := range in.WinChangelogInfos {
		stream, err := sharedlog_stream.NewShardedSharedLogStream(s.env, wsInfo.GetTopicName(),
			uint8(wsInfo.GetNumPartition()), s.serdeFormat, in.GetBufMaxSize())
		if err != nil {
			return nil, err
		}
		tm.RecordTopicStreams(wsInfo.GetTopicName(), stream)
	}
	debug.Fprint(os.Stderr, "init 6\n")
	s.mu.Lock()
	s.tm_map[in.TransactionalId] = tm
	s.prod_id_map[in.TransactionalId] = tm.prodId
	s.mu.Unlock()
	debug.Fprintf(os.Stderr, "[%d] done init %s\n", in.SubstreamNum, in.TransactionalId)
	return &remote_txn_rpc.InitReply{
		ProdId: &commtypes.ProdId{
			TaskId:    tm.prodId.TaskId,
			TaskEpoch: uint32(tm.prodId.TaskEpoch),
		},
		OffsetPairs: offsetPairs,
	}, nil
}

func (s *RemoteTxnManager) AppendTpPar(ctx context.Context, in *txn_data.TxnMetaMsg) (*emptypb.Empty, error) {
	debug.Fprintf(os.Stderr, "handle AppendTpPar with input: %v\n", in)
	s.mu.Lock()
	prodId := s.prod_id_map[in.TransactionalId]
	if prodId.TaskEpoch != in.ProdId.GetTaskEpoch() || prodId.TaskId != in.ProdId.GetTaskId() {
		s.mu.Unlock()
		return nil, common_errors.ErrStaleProducer
	}
	tm := s.tm_map[in.TransactionalId]
	s.mu.Unlock()
	if env_config.ASYNC_SECOND_PHASE && !tm.hasWaitForLastTxn.Load() {
		tBeg := stats.TimerBegin()
		err := tm.bgErrg.Wait()
		if err != nil {
			return nil, err
		}
		tm.waitPrevTxn.AddSample(stats.Elapsed(tBeg).Microseconds())
		tm.bgErrg, tm.bgCtx = errgroup.WithContext(ctx)
	}
	txnMeta := txn_data.TxnMetadata{
		State:           txn_data.TransactionState(in.State),
		TopicPartitions: in.TopicPartitions,
	}
	_, err := tm.appendToTransactionLog(ctx, txnMeta, []uint64{tm.txnLogTag})
	debug.Fprint(os.Stderr, "done AppendTpPar\n")
	if err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

func (s *RemoteTxnManager) AbortTxn(ctx context.Context, in *txn_data.TxnMetaMsg) (*emptypb.Empty, error) {
	debug.Fprintf(os.Stderr, "handle AbortTxn with input: %v\n", in)
	s.mu.Lock()
	prodId := s.prod_id_map[in.TransactionalId]
	if prodId.TaskEpoch != in.ProdId.GetTaskEpoch() || prodId.TaskId != in.ProdId.GetTaskId() {
		s.mu.Unlock()
		return nil, common_errors.ErrStaleProducer
	}
	tm := s.tm_map[in.TransactionalId]
	s.mu.Unlock()
	txnMd := txn_data.TxnMetadata{
		State: txn_data.PREPARE_ABORT,
	}
	_, err := tm.appendToTransactionLog(ctx, txnMd, []uint64{tm.txnLogTag})
	if err != nil {
		return nil, err
	}
	err = tm.completeTransaction(ctx, commtypes.ABORT, txn_data.COMPLETE_ABORT, in.TopicPartitions)
	debug.Fprint(os.Stderr, "done AbortTxn\n")
	if err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

func (s *RemoteTxnManager) AppendConsumedOffset(ctx context.Context, in *remote_txn_rpc.ConsumedOffsets) (*emptypb.Empty, error) {
	debug.Fprintf(os.Stderr, "handle AppendConsumedOffset with input: %v\n", in)
	s.mu.Lock()
	prodId := s.prod_id_map[in.TransactionalId]
	if prodId.TaskEpoch != in.ProdId.GetTaskEpoch() || prodId.TaskId != in.ProdId.GetTaskId() {
		s.mu.Unlock()
		return nil, common_errors.ErrStaleProducer
	}
	tm := s.tm_map[in.TransactionalId]
	s.mu.Unlock()
	var tps []*txn_data.TopicPartition
	for _, op := range in.OffsetPairs {
		tps = append(tps, &txn_data.TopicPartition{
			Topic:  op.TopicName,
			ParNum: []byte{uint8(in.ParNum)},
		})
	}
	txnMeta := txn_data.TxnMetadata{
		State:           txn_data.BEGIN,
		TopicPartitions: tps,
	}
	_, err := tm.appendToTransactionLog(ctx, txnMeta, []uint64{tm.txnLogTag})
	if err != nil {
		return nil, err
	}
	for _, op := range in.OffsetPairs {
		offsetLog := tm.topicStreams[op.TopicName]
		offsetRecord := txn_data.OffsetRecord{
			Offset: op.Offset,
		}
		encoded, err := tm.offsetRecordSerde.Encode(offsetRecord)
		if err != nil {
			return nil, err
		}
		_, err = offsetLog.Push(ctx, encoded, uint8(in.ParNum), sharedlog_stream.SingleDataRecordMeta,
			tm.prodId)
		if err != nil {
			return nil, err
		}
	}
	debug.Fprint(os.Stderr, "done AppendConsumedOffset\n")
	return &emptypb.Empty{}, nil
}

func (s *RemoteTxnManager) CommitTxnAsyncComplete(ctx context.Context, in *txn_data.TxnMetaMsg) (*remote_txn_rpc.CommitReply, error) {
	debug.Fprintf(os.Stderr, "handle CommitTxnAsyncComplete with input: %v\n", in)
	s.mu.Lock()
	prodId := s.prod_id_map[in.TransactionalId]
	if prodId.TaskEpoch != in.ProdId.GetTaskEpoch() || prodId.TaskId != in.ProdId.GetTaskId() {
		s.mu.Unlock()
		return nil, common_errors.ErrStaleProducer
	}
	tm := s.tm_map[in.TransactionalId]
	s.mu.Unlock()
	txnMd := txn_data.TxnMetadata{
		State: txn_data.PREPARE_COMMIT,
	}
	logOff, err := tm.appendToTransactionLog(ctx, txnMd, []uint64{tm.txnLogTag})
	if err != nil {
		return nil, err
	}
	tm.bgErrg.Go(func() error {
		// second phase of the commit
		err = tm.completeTransaction(tm.bgCtx, commtypes.EPOCH_END, txn_data.COMPLETE_COMMIT, in.TopicPartitions)
		if err != nil {
			return err
		}
		tm.hasWaitForLastTxn.Store(true)
		return nil
	})
	debug.Fprint(os.Stderr, "done CommitTxnAsyncComplete\n")
	return &remote_txn_rpc.CommitReply{
		LogOffset: logOff,
	}, nil
}
