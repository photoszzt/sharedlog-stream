package control_channel

import (
	"context"
	"sharedlog-stream/pkg/common_errors"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/data_structure"
	"sharedlog-stream/pkg/hashfuncs"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/store"
	"sharedlog-stream/pkg/store_restore"
	"sync"
	"time"

	// "sharedlog-stream/pkg/stats"
	"sharedlog-stream/pkg/txn_data"
	"sharedlog-stream/pkg/utils/syncutils"

	"cs.utexas.edu/zjia/faas/types"
)

const (
	CONTROL_LOG_TOPIC_NAME = "__control_log"
)

type keyMeta struct {
	hash      uint64
	substream uint8
}

type ControlChannelManager struct {
	kmMu syncutils.Mutex
	// topic -> (key -> set of substreamid)
	keyMappings map[string]map[string]keyMeta

	ctrlMetaSerde   commtypes.SerdeG[txn_data.ControlMetadata]
	payloadArrSerde commtypes.SerdeG[commtypes.PayloadArr]

	// they are the same stream but different progress tracking
	controlLogForRead  *sharedlog_stream.ShardedSharedLogStream // for reading events
	controlLogForWrite *sharedlog_stream.ShardedSharedLogStream // for writing;
	controlOutput      chan ControlChannelResult
	controlQuit        chan struct{}
	// appendCtrlLog      *stats.ConcurrentInt64Collector
	funcName     string
	ctrlMetaTag  uint64
	ctrlLogTag   uint64
	currentEpoch uint16
	instanceID   uint8
}

func (cm *ControlChannelManager) CurrentEpoch() uint16 {
	return cm.currentEpoch
}

func NewControlChannelManager(env types.Environment,
	app_id string,
	serdeFormat commtypes.SerdeFormat,
	epoch uint16,
	instanceID uint8,
) (*ControlChannelManager, error) {
	logForRead, err := sharedlog_stream.NewShardedSharedLogStream(env, CONTROL_LOG_TOPIC_NAME+"_"+app_id, 1, serdeFormat)
	if err != nil {
		return nil, err
	}
	logForWrite, err := sharedlog_stream.NewShardedSharedLogStream(env,
		CONTROL_LOG_TOPIC_NAME+"_"+app_id, 1, serdeFormat)
	if err != nil {
		return nil, err
	}
	cm := &ControlChannelManager{
		controlLogForRead:  logForRead,
		controlLogForWrite: logForWrite,
		currentEpoch:       epoch,
		keyMappings:        make(map[string]map[string]keyMeta),
		funcName:           app_id,
		// appendCtrlLog:      stats.NewConcurrentInt64Collector("append_ctrl_log", stats.DEFAULT_COLLECT_DURATION),
		payloadArrSerde: sharedlog_stream.DEFAULT_PAYLOAD_ARR_SERDEG,
		instanceID:      instanceID,
		ctrlMetaTag:     txn_data.CtrlMetaTag(logForRead.TopicNameHash(), 0),
		ctrlLogTag:      sharedlog_stream.NameHashWithPartition(logForRead.TopicNameHash(), 0),
	}
	// fmt.Fprintf(os.Stderr, "ctrllog name: %s, tag: 0x%x\n", logForRead.TopicName(), cm.ctrlMetaTag)
	ctrlMetaSerde, err := txn_data.GetControlMetadataSerdeG(serdeFormat)
	if err != nil {
		return nil, err
	}
	cm.ctrlMetaSerde = ctrlMetaSerde
	return cm, nil
}

type restoreWork struct {
	topic  string
	isKV   bool
	parNum uint8
}

func (cmm *ControlChannelManager) RestoreMappingAndWaitForPrevTask(
	ctx context.Context, funcName string, kvchangelog map[string]store.KeyValueStoreOpWithChangelog,
	wschangelog map[string]store.WindowStoreOpWithChangelog,
) error {
	extraParToRestoreKV := make(map[string]data_structure.Uint8Set)
	extraParToRestoreWS := make(map[string]data_structure.Uint8Set)
	size := 0
	for _, kvc := range kvchangelog {
		size += int(kvc.Stream().NumPartition())
	}
	for _, wsc := range wschangelog {
		size += int(wsc.Stream().NumPartition())
	}
	var wg sync.WaitGroup
	workChan := make(chan restoreWork, size)
	errChan := make(chan error, 1)
	if size > 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for work := range workChan {
				if work.isKV {
					kvc := kvchangelog[work.topic]
					err := store_restore.RestoreChangelogKVStateStore(ctx, kvc, 0, work.parNum)
					if err != nil {
						errChan <- err
						return
					}
				} else {
					wsc := wschangelog[work.topic]
					err := store_restore.RestoreChangelogWindowStateStore(ctx, wsc, work.parNum)
					if err != nil {
						errChan <- err
						return
					}
				}
			}
		}()
	}
	for {
		rawMsg, err := cmm.controlLogForRead.ReadNext(ctx, 0)
		if err != nil {
			if common_errors.IsStreamEmptyError(err) {
				time.Sleep(time.Duration(100) * time.Millisecond)
				continue
			}
			close(workChan)
			wg.Wait()
			return err
		}
		ctrlMeta, err := cmm.ctrlMetaSerde.Decode(rawMsg.Payload)
		if err != nil {
			close(workChan)
			wg.Wait()
			return err
		}
		if ctrlMeta.Topic != "" {
			cmm.updateKeyMapping(&ctrlMeta)
			kvc, hasKVC := kvchangelog[ctrlMeta.Topic]
			wsc, hasWSC := wschangelog[ctrlMeta.Topic]
			if hasKVC {
				par := uint8(ctrlMeta.Hash % uint64(kvc.Stream().NumPartition()))
				if par != cmm.instanceID {
					pars, ok := extraParToRestoreKV[ctrlMeta.Topic]
					if !ok {
						pars = data_structure.NewUint8Set()
					}
					if !pars.Has(par) {
						workChan <- restoreWork{topic: ctrlMeta.Topic, isKV: true, parNum: par}
						pars.Add(par)
						extraParToRestoreKV[ctrlMeta.Topic] = pars
					}
				}
			} else if hasWSC {
				par := uint8(ctrlMeta.Hash % uint64(wsc.Stream().NumPartition()))
				if par != cmm.instanceID {
					pars, ok := extraParToRestoreWS[ctrlMeta.Topic]
					if !ok {
						pars = data_structure.NewUint8Set()
					}
					if !pars.Has(par) {
						workChan <- restoreWork{topic: ctrlMeta.Topic, isKV: false, parNum: par}
						pars.Add(par)
						extraParToRestoreWS[ctrlMeta.Topic] = pars
					}
				}
			}
		} else if ctrlMeta.FinishedPrevTask != "" {
			// fmt.Fprintf(os.Stderr, "finished prev task %s, funcName %s, meta epoch %d, input epoch %d\n",
			// 	ctrlMeta.FinishedPrevTask, funcName, ctrlMeta.Epoch, cmm.currentEpoch)
			if ctrlMeta.FinishedPrevTask == funcName && ctrlMeta.Epoch+1 == cmm.currentEpoch {
				close(workChan)
				wg.Wait()
				select {
				case err := <-errChan:
					if err != nil {
						return err
					}
				default:
				}
				return nil
			}
		}
	}
}

func (cmm *ControlChannelManager) appendToControlLog(ctx context.Context, cm txn_data.ControlMetadata, allowBuffer bool) error {
	msg_encoded, err := cmm.ctrlMetaSerde.Encode(cm)
	if err != nil {
		return err
	}
	if allowBuffer {
		err = cmm.controlLogForWrite.BufPush(ctx, msg_encoded, 0, commtypes.EmptyProducerId)
		// debug.Fprintf(os.Stderr, "appendToControlLog: tp %s %v, off %x\n",
		// 	cmm.controlLog.TopicName(), cm, off)
	} else {
		err = cmm.controlLogForWrite.Flush(ctx, commtypes.EmptyProducerId)
		if err != nil {
			return err
		}
		// var off uint64
		_, err = cmm.controlLogForWrite.PushWithTag(ctx, msg_encoded, 0, []uint64{cmm.ctrlMetaTag, cmm.ctrlLogTag}, nil,
			sharedlog_stream.SingleDataRecordMeta, commtypes.EmptyProducerId)
		// debug.Fprintf(os.Stderr, "appendToControlLog: cm %+v, tag 0x%x, off 0x%x\n", cm, cmm.ctrlMetaTag, off)
	}
	return err
}

func (cmm *ControlChannelManager) FlushControlLog(ctx context.Context) error {
	return cmm.controlLogForWrite.Flush(ctx, commtypes.EmptyProducerId)
}

func (cmm *ControlChannelManager) AppendRescaleConfig(ctx context.Context,
	config map[string]uint8,
) error {
	cmm.currentEpoch += 1
	cm := txn_data.ControlMetadata{
		Config: config,
		Epoch:  cmm.currentEpoch,
	}
	err := cmm.appendToControlLog(ctx, cm, false)
	return err
}

func TrackAndAppendKeyMapping(
	ctx context.Context,
	cmm *ControlChannelManager,
	kBytes []byte,
	substreamId uint8,
	topic string,
) error {
	cmm.kmMu.Lock()
	subs, ok := cmm.keyMappings[topic]
	if !ok {
		subs = make(map[string]keyMeta)
		cmm.keyMappings[topic] = subs
	}
	_, hasKey := subs[string(kBytes)]
	if !hasKey {
		hasher := hashfuncs.ByteSliceHasher{}
		hash := hasher.HashSum64(kBytes)
		subs[string(kBytes)] = keyMeta{
			substream: substreamId,
			hash:      hash,
		}
		cmm.kmMu.Unlock()
		// apStart := stats.TimerBegin()
		cm := txn_data.ControlMetadata{
			Key:         kBytes,
			Hash:        hash,
			SubstreamId: substreamId,
			Topic:       topic,
			InstanceId:  cmm.instanceID,
			Epoch:       cmm.currentEpoch,
		}
		err := cmm.appendToControlLog(ctx, cm, true)
		// apElapsed := stats.Elapsed(apStart).Microseconds()
		// cmm.appendCtrlLog.AddSample(apElapsed)
		return err
	} else {
		cmm.kmMu.Unlock()
		return nil
	}
}

func (cmm *ControlChannelManager) RecordPrevInstanceFinish(
	ctx context.Context,
	funcName string,
	instanceID uint8,
	epoch uint16,
) error {
	// debug.Fprintf(os.Stderr, "%s(%d) epoch %d finished\n", funcName, instanceID, epoch)
	cm := txn_data.ControlMetadata{
		FinishedPrevTask: funcName,
		InstanceId:       instanceID,
		Epoch:            epoch,
	}
	err := cmm.appendToControlLog(ctx, cm, false)
	return err
}

func (cmm *ControlChannelManager) updateKeyMapping(ctrlMeta *txn_data.ControlMetadata) {
	cmm.kmMu.Lock()
	subs, ok := cmm.keyMappings[ctrlMeta.Topic]
	if !ok {
		subs = make(map[string]keyMeta)
		cmm.keyMappings[ctrlMeta.Topic] = subs
	}
	_, hasKey := subs[string(ctrlMeta.Key)]
	if !hasKey {
		subs[string(ctrlMeta.Key)] = keyMeta{
			substream: ctrlMeta.SubstreamId,
			hash:      ctrlMeta.Hash,
		}
	}
	cmm.kmMu.Unlock()
}

func (cmm *ControlChannelManager) StartMonitorControlChannel(ctx context.Context) {
	cmm.controlQuit = make(chan struct{})
	cmm.controlOutput = make(chan ControlChannelResult, 10)
	go cmm.monitorControlChannel(ctx, cmm.controlQuit, cmm.controlOutput)
}

func (cmm *ControlChannelManager) OutputChan() chan ControlChannelResult {
	return cmm.controlOutput
}

func (cmm *ControlChannelManager) SendQuit() {
	cmm.controlQuit <- struct{}{}
}

func (cmm *ControlChannelManager) monitorControlChannel(
	ctx context.Context,
	quit <-chan struct{},
	output chan<- ControlChannelResult,
) {
	// fmt.Fprintf(os.Stderr, "start monitoring control channel from %s 0x%x with tag 0x%x\n",
	// 	cmm.controlLogForRead.TopicName(), cmm.controlLogForRead.GetCuror(0), cmm.ctrlMetaTag)
	for {
		select {
		case <-quit:
			return
		default:
		}

		rawMsg, err := cmm.controlLogForRead.ReadNextWithTag(ctx, 0, cmm.ctrlMetaTag)
		if err != nil {
			if common_errors.IsStreamEmptyError(err) {
				time.Sleep(time.Duration(1) * time.Millisecond)
				continue
			}
			output <- ControlChannelErr(err)
			break
		}
		// fmt.Fprintf(os.Stderr, "read control message from 0x%x\n", rawMsg.LogSeqNum)
		if !rawMsg.IsPayloadArr {
			ctrlMeta, err := cmm.ctrlMetaSerde.Decode(rawMsg.Payload)
			if err != nil {
				output <- ControlChannelErr(err)
				break
			}
			// fmt.Fprintf(os.Stderr, "monitorControlChannel: %+v\n", ctrlMeta)
			output <- ControlChannelVal(&ctrlMeta)
		}
	}
}
