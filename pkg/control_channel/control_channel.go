package control_channel

import (
	"context"
	"sharedlog-stream/pkg/common_errors"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/data_structure"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/stats"
	"sharedlog-stream/pkg/txn_data"
	"sharedlog-stream/pkg/utils/syncutils"

	"cs.utexas.edu/zjia/faas/types"
)

const (
	CONTROL_LOG_TOPIC_NAME = "__control_log"
)

type ControlChannelManager struct {
	kmMu syncutils.Mutex
	// topic -> (key -> set of substreamid)
	keyMappings map[string]map[string]data_structure.Uint8Set

	msgSerde        commtypes.MessageSerdeG[string, txn_data.ControlMetadata]
	payloadArrSerde commtypes.SerdeG[commtypes.PayloadArr]

	topicStreams map[string]*sharedlog_stream.ShardedSharedLogStream
	// they are the same stream but different progress tracking
	controlLogForRead  *sharedlog_stream.ShardedSharedLogStream // for reading events
	controlLogForWrite *sharedlog_stream.ShardedSharedLogStream // for writing;
	controlOutput      chan ControlChannelResult
	controlQuit        chan struct{}
	appendCtrlLog      *stats.ConcurrentInt64Collector
	funcName           string
	currentEpoch       uint64
	instanceID         uint8
}

func (cm *ControlChannelManager) CurrentEpoch() uint64 {
	return cm.currentEpoch
}

func NewControlChannelManager(env types.Environment,
	app_id string,
	serdeFormat commtypes.SerdeFormat,
	epoch uint64,
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
		currentEpoch:       0,
		topicStreams:       make(map[string]*sharedlog_stream.ShardedSharedLogStream),
		keyMappings:        make(map[string]map[string]data_structure.Uint8Set),
		funcName:           app_id,
		appendCtrlLog:      stats.NewConcurrentInt64Collector("append_ctrl_log", stats.DEFAULT_COLLECT_DURATION),
		payloadArrSerde:    sharedlog_stream.DEFAULT_PAYLOAD_ARR_SERDEG,
		instanceID:         instanceID,
	}
	ctrlMetaSerde, err := txn_data.GetControlMetadataSerdeG(serdeFormat)
	if err != nil {
		return nil, err
	}
	cm.msgSerde, err = commtypes.GetMsgSerdeG[string](serdeFormat, commtypes.StringSerdeG{}, ctrlMetaSerde)
	if err != nil {
		return nil, err
	}
	return cm, nil
}

func (cmm *ControlChannelManager) RestoreMapping(ctx context.Context) error {
	for {
		rawMsg, err := cmm.controlLogForRead.ReadNext(ctx, 0)
		if err != nil {
			if common_errors.IsStreamEmptyError(err) {
				return nil
			}
			return err
		}
		msg, err := cmm.msgSerde.Decode(rawMsg.Payload)
		if err != nil {
			return err
		}
		ctrlMeta := msg.Value.(txn_data.ControlMetadata)
		if ctrlMeta.Config == nil {
			cmm.currentEpoch = ctrlMeta.Epoch
			cmm.updateKeyMapping(&ctrlMeta)
		}
	}
}

func (cmm *ControlChannelManager) TrackStream(topicName string, stream *sharedlog_stream.ShardedSharedLogStream) {
	cmm.topicStreams[topicName] = stream
}

func (cmm *ControlChannelManager) appendToControlLog(ctx context.Context, cm txn_data.ControlMetadata, allowBuffer bool) error {
	msg_encoded, err := cmm.msgSerde.Encode(commtypes.Message{Key: "", Value: cm})
	if err != nil {
		return err
	}
	if allowBuffer {
		err = cmm.controlLogForWrite.BufPush(ctx, msg_encoded, 0, commtypes.EmptyProducerId)
		// debug.Fprintf(os.Stderr, "appendToControlLog: tp %s %v, off %x\n",
		// 	cmm.controlLog.TopicName(), cm, off)
	} else {
		_, err = cmm.controlLogForWrite.Push(ctx, msg_encoded, 0, sharedlog_stream.SingleDataRecordMeta,
			commtypes.EmptyProducerId)
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
	key interface{},
	keySerde commtypes.Encoder,
	substreamId uint8,
	topic string,
) error {
	kBytes, err := keySerde.Encode(key)
	if err != nil {
		return err
	}
	cmm.kmMu.Lock()
	subs, ok := cmm.keyMappings[topic]
	if !ok {
		subs = make(map[string]data_structure.Uint8Set)
		cmm.keyMappings[topic] = subs
	}
	sub, ok := subs[string(kBytes)]
	if !ok {
		sub = make(data_structure.Uint8Set)
	}
	if !sub.Has(substreamId) {
		sub.Add(substreamId)
		subs[string(kBytes)] = sub
		cmm.kmMu.Unlock()
		apStart := stats.TimerBegin()
		cm := txn_data.ControlMetadata{
			Key:         kBytes,
			SubstreamId: substreamId,
			Topic:       topic,
			InstanceId:  cmm.instanceID,
			Epoch:       cmm.currentEpoch,
		}
		err = cmm.appendToControlLog(ctx, cm, true)
		apElapsed := stats.Elapsed(apStart).Microseconds()
		cmm.appendCtrlLog.AddSample(apElapsed)
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
	epoch uint64,
) error {
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
		subs = make(map[string]data_structure.Uint8Set)
		cmm.keyMappings[ctrlMeta.Topic] = subs
	}
	sub, ok := subs[string(ctrlMeta.Key)]
	if !ok {
		sub = make(data_structure.Uint8Set)
	}
	if !sub.Has(ctrlMeta.SubstreamId) {
		sub.Add(ctrlMeta.SubstreamId)
		subs[string(ctrlMeta.Key)] = sub
	}
	cmm.kmMu.Unlock()
}

func (cmm *ControlChannelManager) StartMonitorControlChannel(ctx context.Context) {
	cmm.controlQuit = make(chan struct{})
	cmm.controlOutput = make(chan ControlChannelResult, 1)
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
	for {
		select {
		case <-quit:
			return
		default:
		}

		rawMsg, err := cmm.controlLogForRead.ReadNext(ctx, 0)
		if err != nil {
			if common_errors.IsStreamEmptyError(err) {
				continue
			}
			output <- ControlChannelErr(err)
			break
		} else {
			if !rawMsg.IsPayloadArr {
				msg, err := cmm.msgSerde.Decode(rawMsg.Payload)
				if err != nil {
					output <- ControlChannelErr(err)
					break
				}
				ctrlMeta := msg.Value.(txn_data.ControlMetadata)
				if ctrlMeta.Key == nil {
					output <- ControlChannelVal(&ctrlMeta)
				}
			}
		}
	}
}
