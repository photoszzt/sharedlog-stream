package control_channel

import (
	"bytes"
	"context"
	"fmt"
	"sharedlog-stream/pkg/common_errors"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/data_structure"
	"sharedlog-stream/pkg/hashfuncs"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/snapshot_store"
	"sharedlog-stream/pkg/store"
	"sharedlog-stream/pkg/store_restore"
	"time"

	// "sharedlog-stream/pkg/stats"
	"sharedlog-stream/pkg/txn_data"

	"cs.utexas.edu/zjia/faas/types"
	"github.com/zhangyunhao116/skipmap"
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
	// kmMu               syncutils.Mutex
	payloadArrSerde    commtypes.SerdeG[commtypes.PayloadArr]
	ctrlMetaSerde      commtypes.SerdeG[txn_data.ControlMetadata]
	uint16SerdeG       commtypes.SerdeG[uint16]
	payloadArrSerdeG   commtypes.SerdeG[commtypes.PayloadArr]
	controlOutput      chan ControlChannelResult
	controlLogForRead  *sharedlog_stream.ShardedSharedLogStream
	controlLogForWrite *sharedlog_stream.ShardedSharedLogStream
	// topic -> (key -> set of substreamid)
	// keyMappings  map[string]map[string]keyMeta // protected by kmMu
	keyMappings  *skipmap.StringMap[*skipmap.FuncMap[[]byte, keyMeta]]
	controlQuit  chan struct{}
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
	payloadSerde, err := commtypes.GetPayloadArrSerdeG(serdeFormat)
	if err != nil {
		return nil, err
	}
	cm := &ControlChannelManager{
		uint16SerdeG:       commtypes.Uint16SerdeG{},
		payloadArrSerdeG:   payloadSerde,
		controlLogForRead:  logForRead,
		controlLogForWrite: logForWrite,
		currentEpoch:       epoch,
		// keyMappings:        make(map[string]map[string]keyMeta),
		keyMappings: skipmap.NewString[*skipmap.FuncMap[[]byte, keyMeta]](),
		funcName:    app_id,
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

func (cmm *ControlChannelManager) loadAndDecodeSnapshot(
	ctx context.Context, topic string,
	rs *snapshot_store.RedisSnapshotStore,
	auxData []byte, metaSeqNum uint64,
) ([][]byte, error) {
	ret, err := cmm.uint16SerdeG.Decode(auxData)
	if err != nil {
		return nil, fmt.Errorf("[ERR] Decode: %v", err)
	}
	if ret == 1 {
		snapArr, err := rs.GetSnapshot(ctx, topic, metaSeqNum)
		if err != nil {
			return nil, err
		}
		if len(snapArr) > 0 {
			payloadArr, err := cmm.payloadArrSerde.Decode(snapArr)
			if err != nil {
				return nil, err
			}
			return payloadArr.Payloads, nil
		}
	}
	return nil, nil
}

func (cmm *ControlChannelManager) loadSnapshotToKV(
	ctx context.Context,
	work restoreWork,
	kvc store.KeyValueStoreOpWithChangelog,
	rs *snapshot_store.RedisSnapshotStore,
) error {
	auxData, metaSeqNum, err := kvc.FindLastEpochMetaWithAuxData(ctx, work.parNum)
	if err != nil {
		return err
	}
	payloads, err := cmm.loadAndDecodeSnapshot(ctx, work.topic, rs, auxData, metaSeqNum)
	if err != nil {
		return err
	}
	if len(payloads) > 0 {
		err = kvc.RestoreFromSnapshot(payloads)
		if err != nil {
			return err
		}
		kvc.Stream().SetCursor(metaSeqNum+1, kvc.SubstreamNum())
	}
	return nil
}

func (cmm *ControlChannelManager) loadSnapshotToWinStore(
	ctx context.Context,
	work restoreWork,
	wsc store.WindowStoreOpWithChangelog,
	rs *snapshot_store.RedisSnapshotStore,
) error {
	auxData, metaSeqNum, err := wsc.FindLastEpochMetaWithAuxData(ctx, work.parNum)
	if err != nil {
		return err
	}
	payloads, err := cmm.loadAndDecodeSnapshot(ctx, work.topic, rs, auxData, metaSeqNum)
	if err != nil {
		return err
	}
	if len(payloads) > 0 {
		err = wsc.RestoreFromSnapshot(ctx, payloads)
		if err != nil {
			return err
		}
		wsc.Stream().SetCursor(metaSeqNum+1, wsc.SubstreamNum())
	}
	return nil
}

func (cmm *ControlChannelManager) RestoreMappingAndWaitForPrevTask(
	ctx context.Context, funcName string,
	createSnapshot bool,
	serdeFormat commtypes.SerdeFormat,
	kvchangelog map[string]store.KeyValueStoreOpWithChangelog,
	wschangelog map[string]store.WindowStoreOpWithChangelog,
	rs *snapshot_store.RedisSnapshotStore,
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
			if createSnapshot {
				err := cmm.loadSnapshotToKV(bgCtx, work, kvc, rs)
				if err != nil {
					return err
				}
			}
			err := store_restore.RestoreChangelogKVStateStore(bgCtx, kvc, 0, work.parNum)
			if err != nil {
				return err
			}
		} else {
			wsc := wschangelog[work.topic]
			if createSnapshot {
				err := cmm.loadSnapshotToWinStore(bgCtx, work, wsc, rs)
				if err != nil {
					return err
				}
			}
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
				for tp, kms := range ctrlMeta.KeyMaps {
					kvc, hasKVC := kvchangelog[tp]
					wsc, hasWSC := wschangelog[tp]
					if hasKVC {
						for _, km := range kms {
							// compute the new key assignment
							par := uint8(km.Hash % uint64(kvc.Stream().NumPartition()))
							// if this key is managed by this node
							if par == cmm.instanceID {
								pars, ok := extraParToRestoreKV[tp]
								if !ok {
									pars = data_structure.NewUint8Set()
								}
								if !pars.Has(km.SubstreamId) {
									// fmt.Fprintf(os.Stderr, "restore %s par %d\n", km.Topic, km.SubstreamId)
									w := restoreWork{topic: tp, isKV: true, parNum: km.SubstreamId}
									bgGrp.Go(func() error {
										return restoreFunc(w)
									})
									pars.Add(km.SubstreamId)
									extraParToRestoreKV[tp] = pars
								}
							}
						}
					} else if hasWSC {
						for _, km := range kms {
							par := uint8(km.Hash % uint64(wsc.Stream().NumPartition()))
							if par == cmm.instanceID {
								pars, ok := extraParToRestoreWS[tp]
								if !ok {
									pars = data_structure.NewUint8Set()
								}
								if !pars.Has(km.SubstreamId) {
									// fmt.Fprintf(os.Stderr, "restore %s par %d\n", km.Topic, km.SubstreamId)
									w := restoreWork{topic: tp, isKV: false, parNum: km.SubstreamId}
									bgGrp.Go(func() error {
										return restoreFunc(w)
									})
									pars.Add(km.SubstreamId)
									extraParToRestoreWS[tp] = pars
								}
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

func (cmm *ControlChannelManager) FlushKeyMapping(ctx context.Context) error {
	/*
		if len(cmm.keyMappings) > 0 {
			kms := make(map[string][]txn_data.KeyMaping, len(cmm.keyMappings))
			for k, v := range cmm.keyMappings {
				km, ok := kms[k]
				if !ok {
					km = make([]txn_data.KeyMaping, 0)
				}
				for kBytes, meta := range v {
					km = append(km, txn_data.KeyMaping{
						Key:         []byte(kBytes),
						Hash:        meta.hash,
						SubstreamId: meta.substream,
					})
				}
			}
			cm := txn_data.ControlMetadata{
				KeyMaps:    kms,
				InstanceId: cmm.instanceID,
				Epoch:      cmm.currentEpoch,
			}
			err := cmm.appendToControlLog(ctx, cm, false)
			if err != nil {
				return err
			}
		}
	*/
	if cmm.keyMappings.Len() > 0 {
		kms := make(map[string][]txn_data.KeyMaping, cmm.keyMappings.Len())
		cmm.keyMappings.Range(func(key string, value *skipmap.FuncMap[[]byte, keyMeta]) bool {
			km, ok := kms[key]
			if !ok {
				km = make([]txn_data.KeyMaping, 0)
			}
			value.Range(func(key []byte, value keyMeta) bool {
				km = append(km, txn_data.KeyMaping{
					Key:         key,
					Hash:        value.hash,
					SubstreamId: value.substream,
				})
				return true
			})
			return true
		})
		cm := txn_data.ControlMetadata{
			KeyMaps:    kms,
			InstanceId: cmm.instanceID,
			Epoch:      cmm.currentEpoch,
		}
		err := cmm.appendToControlLog(ctx, cm, false)
		if err != nil {
			return err
		}
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
	/*
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
		} else {
			cmm.kmMu.Unlock()
		}
	*/
	subs, _ := cmm.keyMappings.LoadOrStore(topic, skipmap.NewFunc[[]byte, keyMeta](func(a, b []byte) bool {
		return bytes.Compare(a, b) < 0
	}))
	subs.LoadOrStoreLazy(kBytes, func() keyMeta {
		hasher := hashfuncs.ByteSliceHasher{}
		hash := hasher.HashSum64(kBytes)
		return keyMeta{
			substream: substreamId,
			hash:      hash,
		}
	})
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
	/*
		cmm.kmMu.Lock()
		for tp, kms := range ctrlMeta.KeyMaps {
			subs, ok := cmm.keyMappings[tp]
			if !ok {
				subs = make(map[string]keyMeta)
				cmm.keyMappings[tp] = subs
			}
			for _, km := range kms {
				_, hasKey := subs[string(km.Key)]
				if !hasKey {
					subs[string(km.Key)] = keyMeta{
						substream: km.SubstreamId,
						hash:      km.Hash,
					}
				}
			}
		}
		cmm.kmMu.Unlock()
	*/
	for tp, kms := range ctrlMeta.KeyMaps {
		subs, _ := cmm.keyMappings.LoadOrStore(tp, skipmap.NewFunc[[]byte, keyMeta](func(a, b []byte) bool {
			return bytes.Compare(a, b) < 0
		}))
		for _, km := range kms {
			subs.LoadOrStoreLazy(km.Key, func() keyMeta {
				hasher := hashfuncs.ByteSliceHasher{}
				hash := hasher.HashSum64(km.Key)
				return keyMeta{
					substream: km.SubstreamId,
					hash:      hash,
				}
			})
		}
	}
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
