package store_with_changelog

import (
	"context"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/source_sink"
	"sharedlog-stream/pkg/transaction/tran_interface"
	"sharedlog-stream/pkg/utils"

	"cs.utexas.edu/zjia/faas/types"
)

// a changelog substream has only one producer. Each store would only produce to one substream.
type ChangelogManager struct {
	tm            tran_interface.ReadOnlyExactlyOnceManager
	changelog     *sharedlog_stream.ShardedSharedLogStream
	tac           *source_sink.TransactionAwareConsumer
	bufPush       bool
	transactional bool
	serdeFormat   commtypes.SerdeFormat
}

func NewChangelogManager(stream *sharedlog_stream.ShardedSharedLogStream, serdeFormat commtypes.SerdeFormat) *ChangelogManager {
	return &ChangelogManager{
		tm:            nil,
		changelog:     stream,
		tac:           nil,
		bufPush:       utils.CheckBufPush(),
		transactional: false,
		serdeFormat:   serdeFormat,
	}
}

func (cm *ChangelogManager) TopicName() string {
	return cm.changelog.TopicName()
}

func (cm *ChangelogManager) NumPartition() uint8 {
	return cm.changelog.NumPartition()
}

func (cm *ChangelogManager) InTransaction(tm tran_interface.ReadOnlyExactlyOnceManager) error {
	cm.transactional = true
	cm.tm = tm
	var err error = nil
	cm.tac, err = source_sink.NewTransactionAwareConsumer(cm.changelog, cm.serdeFormat)
	return err
}

func (cm *ChangelogManager) Stream() *sharedlog_stream.ShardedSharedLogStream {
	return cm.changelog
}

func (cm *ChangelogManager) ReadNext(ctx context.Context, parNum uint8) (*commtypes.RawMsg, error) {
	if cm.transactional {
		return cm.tac.ReadNext(ctx, parNum)
	} else {
		return cm.changelog.ReadNext(ctx, parNum)
	}
}

func (cm *ChangelogManager) Push(ctx context.Context, payload []byte, parNum uint8) error {
	if cm.bufPush {
		return cm.bufPushHelper(ctx, payload, parNum)
	} else {
		return cm.pushHelper(ctx, payload, parNum)
	}
}

func (cm *ChangelogManager) bufPushHelper(ctx context.Context, payload []byte, parNum uint8) error {
	if cm.transactional {
		return cm.changelog.BufPush(ctx, payload, parNum, cm.tm.GetCurrentTaskId(), cm.tm.GetCurrentEpoch(), cm.tm.GetTransactionID())
	} else {
		return cm.changelog.BufPush(ctx, payload, parNum, 0, 0, 0)
	}
}

func (cm *ChangelogManager) pushHelper(ctx context.Context, payload []byte, parNum uint8) error {
	if cm.transactional {
		_, err := cm.changelog.Push(ctx, payload, parNum, false, false, cm.tm.GetCurrentTaskId(), cm.tm.GetCurrentEpoch(), cm.tm.GetTransactionID())
		return err
	} else {
		_, err := cm.changelog.Push(ctx, payload, parNum, false, false, 0, 0, 0)
		return err
	}
}

func (cm *ChangelogManager) Flush(ctx context.Context) error {
	if cm.transactional {
		return cm.changelog.FlushNoLock(ctx, cm.tm.GetCurrentTaskId(), cm.tm.GetCurrentEpoch(), cm.tm.GetTransactionID())
	} else {
		return cm.changelog.FlushNoLock(ctx, 0, 0, 0)
	}
}

func CreateChangelog(env types.Environment, tabName string,
	numPartition uint8, serdeFormat commtypes.SerdeFormat,
) (*sharedlog_stream.ShardedSharedLogStream, error) {
	changelog_name := tabName + "-changelog"
	return sharedlog_stream.NewShardedSharedLogStream(env, changelog_name, numPartition, serdeFormat)
}
