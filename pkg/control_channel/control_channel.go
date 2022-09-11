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
	"time"

	// "sharedlog-stream/pkg/stats"
	"sharedlog-stream/pkg/txn_data"
	"sharedlog-stream/pkg/utils/syncutils"

	"cs.utexas.edu/zjia/faas/types"
	"golang.org/x/sync/errgroup"
)

const (
	CONTROL_LOG_TOPIC_NAME = "__control_log"
)

type keyMeta struct {
	hash      uint64
	substream uint8
}

type ControlChannelManager struct {
	kmMu               syncutils.Mutex
	payloadArrSerde    commtypes.SerdeG[commtypes.PayloadArr]
	ctrlMetaSerde      commtypes.SerdeG[txn_data.ControlMetadata]
	controlOutput      chan ControlChannelResult
	controlLogForRead  *sharedlog_stream.ShardedSharedLogStream
	controlLogForWrite *sharedlog_stream.ShardedSharedLogStream
	// topic -> (key -> set of substreamid)
	keyMappings        map[string]map[string]keyMeta // protected by kmMu
	controlQuit        chan struct{}
	funcName           string
	currentKeyMappings []txn_data.KeyMaping
	ctrlMetaTag        uint64
	ctrlLogTag         uint64
	currentEpoch       uint16
	instanceID         uint8
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
		payloadArrSerde:    sharedlog_stream.DEFAULT_PAYLOAD_ARR_SERDEG,
		instanceID:         instanceID,
		ctrlMetaTag:        txn_data.CtrlMetaTag(logForRead.TopicNameHash(), 0),
		ctrlLogTag:         sharedlog_stream.NameHashWithPartition(logForRead.TopicNameHash(), 0),
		currentKeyMappings: make([]txn_data.KeyMaping, 0, 16),
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
	bgGrp, bgCtx := errgroup.WithContext(ctx)
	restoreFunc := func(work restoreWork) error {
		if work.isKV {
			kvc := kvchangelog[work.topic]
			err := store_restore.RestoreChangelogKVStateStore(bgCtx, kvc, 0, work.parNum)
			if err != nil {
				return err
			}
		} else {
			wsc := wschangelog[work.topic]
			err := store_restore.RestoreChangelogWindowStateStore(bgCtx, wsc, work.parNum)
			if err != nil {
				return err
			}
		}
		return nil
	}
	for {
		rawMsg, err := cmm.controlLogForRead.ReadNext(ctx, 0)
		if err != nil {
			if common_errors.IsStreamEmptyError(err) {
				time.Sleep(time.Duration(100) * time.Millisecond)
				continue
			}
			bgErr := bgGrp.Wait()
			if bgErr != nil {
				return bgErr
			}
			return err
		}
		ctrlMeta, err := cmm.ctrlMetaSerde.Decode(rawMsg.Payload)
		if err != nil {
			bgErr := bgGrp.Wait()
			if bgErr != nil {
				return bgErr
			}
			return err
		}
		if ctrlMeta.FinishedPrevTask != "" {
			// fmt.Fprintf(os.Stderr, "finished prev task %s, funcName %s, meta epoch %d, input epoch %d\n",
			// 	ctrlMeta.FinishedPrevTask, funcName, ctrlMeta.Epoch, cmm.currentEpoch)
			if ctrlMeta.FinishedPrevTask == funcName && ctrlMeta.Epoch+1 == cmm.currentEpoch {
				// fmt.Fprintf(os.Stderr, "waiting bg to finish\n")
				err = bgGrp.Wait()
				if err != nil {
					return err
				}
				return nil
			}
		} else {
			if len(ctrlMeta.KeyMaps) != 0 {
				cmm.updateKeyMapping(&ctrlMeta)
				for _, km := range ctrlMeta.KeyMaps {
					kvc, hasKVC := kvchangelog[km.Topic]
					wsc, hasWSC := wschangelog[km.Topic]
					if hasKVC {
						par := uint8(km.Hash % uint64(kvc.Stream().NumPartition()))
						if par != cmm.instanceID {
							pars, ok := extraParToRestoreKV[km.Topic]
							if !ok {
								pars = data_structure.NewUint8Set()
							}
							if !pars.Has(km.SubstreamId) {
								// fmt.Fprintf(os.Stderr, "restore %s par %d\n", km.Topic, km.SubstreamId)
								w := restoreWork{topic: km.Topic, isKV: true, parNum: km.SubstreamId}
								bgGrp.Go(func() error {
									return restoreFunc(w)
								})
								pars.Add(km.SubstreamId)
								extraParToRestoreKV[km.Topic] = pars
							}
						}
					} else if hasWSC {
						par := uint8(km.Hash % uint64(wsc.Stream().NumPartition()))
						if par != cmm.instanceID {
							pars, ok := extraParToRestoreWS[km.Topic]
							if !ok {
								pars = data_structure.NewUint8Set()
							}
							if !pars.Has(km.SubstreamId) {
								// fmt.Fprintf(os.Stderr, "restore %s par %d\n", km.Topic, km.SubstreamId)
								w := restoreWork{topic: km.Topic, isKV: false, parNum: km.SubstreamId}
								bgGrp.Go(func() error {
									return restoreFunc(w)
								})
								pars.Add(km.SubstreamId)
								extraParToRestoreWS[km.Topic] = pars
							}
						}
					}
				}
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
	if len(cmm.currentKeyMappings) > 0 {
		cm := txn_data.ControlMetadata{
			KeyMaps:    cmm.currentKeyMappings,
			InstanceId: cmm.instanceID,
			Epoch:      cmm.currentEpoch,
		}
		err := cmm.appendToControlLog(ctx, cm, false)
		if err != nil {
			return err
		}
		cmm.currentKeyMappings = make([]txn_data.KeyMaping, 0, 16)
	}
	return nil
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
) {
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
		km := txn_data.KeyMaping{
			Key:         kBytes,
			Hash:        hash,
			SubstreamId: substreamId,
			Topic:       topic,
		}
		cmm.currentKeyMappings = append(cmm.currentKeyMappings, km)
		// apElapsed := stats.Elapsed(apStart).Microseconds()
		// cmm.appendCtrlLog.AddSample(apElapsed)
	} else {
		cmm.kmMu.Unlock()
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
	for _, km := range ctrlMeta.KeyMaps {
		subs, ok := cmm.keyMappings[km.Topic]
		if !ok {
			subs = make(map[string]keyMeta)
			cmm.keyMappings[km.Topic] = subs
		}
		_, hasKey := subs[string(km.Key)]
		if !hasKey {
			subs[string(km.Key)] = keyMeta{
				substream: km.SubstreamId,
				hash:      km.Hash,
			}
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
