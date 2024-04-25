package transaction

import (
	"context"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/env_config"
	"sharedlog-stream/pkg/exactly_once_intr"
	"sharedlog-stream/pkg/stats"
	"sharedlog-stream/pkg/transaction/remote_txn_rpc"
	"sharedlog-stream/pkg/txn_data"
	"sync/atomic"

	"github.com/zhangyunhao116/skipmap"
	"github.com/zhangyunhao116/skipset"
	"google.golang.org/grpc"
)

type RemoteTxnManagerClient struct {
	remote_txn_rpc.RemoteTxnMngrClient

	prodId                commtypes.ProducerId
	currentTopicSubstream *skipmap.StringMap[*skipset.Uint32Set]
	TransactionalId       string
	addedNewTpPar         atomic.Bool
}

func NewRemoteTxnManagerClient(cc grpc.ClientConnInterface) *RemoteTxnManagerClient {
	c := &RemoteTxnManagerClient{
		RemoteTxnMngrClient:   remote_txn_rpc.NewRemoteTxnMngrClient(cc),
		currentTopicSubstream: skipmap.NewString[*skipset.Uint32Set](),
	}
	c.addedNewTpPar.Store(false)
	return c
}

var _ = exactly_once_intr.ReadOnlyExactlyOnceManager(&RemoteTxnManagerClient{})

func (tm *RemoteTxnManagerClient) GetCurrentEpoch() uint32             { return tm.prodId.TaskEpoch }
func (tm *RemoteTxnManagerClient) GetCurrentTaskId() uint64            { return tm.prodId.TaskId }
func (tm *RemoteTxnManagerClient) GetProducerId() commtypes.ProducerId { return tm.prodId }
func (tm *RemoteTxnManagerClient) UpdateProducerId(prodId *commtypes.ProdId) {
	tm.prodId.TaskId = prodId.TaskId
	tm.prodId.TaskEpoch = prodId.TaskEpoch
}

func (tc *RemoteTxnManagerClient) AddTopicSubstream(topic string, subStreamNum uint8) {
	// debug.Fprintf(os.Stderr, "tracking topic %s par %v\n", topic, partitions)
	parSet, loaded := tc.currentTopicSubstream.LoadOrStore(topic, skipset.NewUint32())
	needToAppendToLog := !loaded
	hasPar := parSet.Contains(uint32(subStreamNum))
	if !hasPar {
		needToAppendToLog = true
		parSet.Add(uint32(subStreamNum))
	}
	if needToAppendToLog {
		tc.addedNewTpPar.Store(true)
	}
}

func (tc *RemoteTxnManagerClient) EnsurePrevTxnFinAndAppendMeta(ctx context.Context) error {
	if env_config.ASYNC_SECOND_PHASE {
		// TODO: rpc wait for prev finish
	}
	if tc.addedNewTpPar.Load() {
		tBeg := stats.TimerBegin()
		tps := make([]txn_data.TopicPartition, 0, tc.currentTopicSubstream.Len())
		tc.currentTopicSubstream.Range(func(key string, value *skipset.Uint32Set) bool {
			pars := make([]uint8, 0, value.Len())
			value.Range(func(par uint32) bool {
				pars = append(pars, uint8(par))
				return true
			})
			tps = append(tps, txn_data.TopicPartition{
				Topic:  key,
				ParNum: pars,
			})
			return true
		})
		txnMeta := txn_data.TxnMetadata{
			State:           txn_data.BEGIN,
			TopicPartitions: tps,
		}
	}
	return nil
}
