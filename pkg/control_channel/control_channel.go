package control_channel

import (
	"context"
	"fmt"
	"sharedlog-stream/pkg/common_errors"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/txn_data"
	"sync"

	"cs.utexas.edu/zjia/faas/types"
	"github.com/huandu/skiplist"
)

const (
	CONTROL_LOG_TOPIC_NAME = "__control_log"
)

type ControlChannelManager struct {
	controlMetaSerde commtypes.Serde
	msgSerde         commtypes.MsgSerde
	controlLog       *sharedlog_stream.SharedLogStream

	kmMu sync.Mutex
	// topic -> (key -> set of substreamid)
	keyMappings map[string]*skiplist.SkipList

	topicStreams map[string]*sharedlog_stream.ShardedSharedLogStream

	funcName     string
	currentEpoch uint64

	controlOutput chan ControlChannelResult
	controlQuit   chan struct{}
}

func (cm *ControlChannelManager) CurrentEpoch() uint64 {
	return cm.currentEpoch
}

func NewControlChannelManager(env types.Environment,
	app_id string,
	serdeFormat commtypes.SerdeFormat,
	epoch uint64,
) (*ControlChannelManager, error) {
	log, err := sharedlog_stream.NewSharedLogStream(env, app_id+CONTROL_LOG_TOPIC_NAME, serdeFormat)
	if err != nil {
		return nil, err
	}
	cm := &ControlChannelManager{
		controlLog:   log,
		currentEpoch: 0,
		topicStreams: make(map[string]*sharedlog_stream.ShardedSharedLogStream),
		keyMappings:  make(map[string]*skiplist.SkipList),
		funcName:     app_id,
	}
	if serdeFormat == commtypes.JSON {
		cm.controlMetaSerde = txn_data.ControlMetadataJSONSerde{}
		cm.msgSerde = commtypes.MessageSerializedJSONSerde{}
	} else if serdeFormat == commtypes.MSGP {
		cm.controlMetaSerde = txn_data.ControlMetadataMsgpSerde{}
		cm.msgSerde = commtypes.MessageSerializedMsgpSerde{}
	} else {
		return nil, fmt.Errorf("serde format should be either json or msgp; but %v is given", serdeFormat)
	}
	return cm, nil
}

func (cmm *ControlChannelManager) RestoreMapping(ctx context.Context) error {
	for {
		rawMsg, err := cmm.controlLog.ReadNext(ctx, 0)
		if err != nil {
			if common_errors.IsStreamEmptyError(err) {
				return nil
			}
			return err
		}
		_, valBytes, err := cmm.msgSerde.Decode(rawMsg.Payload)
		if err != nil {
			return err
		}
		ctrlMetaTmp, err := cmm.controlMetaSerde.Decode(valBytes)
		if err != nil {
			return err
		}
		ctrlMeta := ctrlMetaTmp.(txn_data.ControlMetadata)
		if ctrlMeta.Config == nil {
			cmm.currentEpoch = ctrlMeta.Epoch
			cmm.updateKeyMapping(&ctrlMeta)
		}
	}
}

func (cmm *ControlChannelManager) TrackStream(topicName string, stream *sharedlog_stream.ShardedSharedLogStream) {
	cmm.topicStreams[topicName] = stream
}

func (cmm *ControlChannelManager) appendToControlLog(ctx context.Context, cm *txn_data.ControlMetadata) error {
	encoded, err := cmm.controlMetaSerde.Encode(cm)
	if err != nil {
		return err
	}
	msg_encoded, err := cmm.msgSerde.Encode(nil, encoded)
	if err != nil {
		return err
	}
	_, err = cmm.controlLog.Push(ctx, msg_encoded, 0, sharedlog_stream.SingleDataRecordMeta, commtypes.EmptyProducerId)
	// debug.Fprintf(os.Stderr, "appendToControlLog: tp %s %v, off %x\n",
	// 	cmm.controlLog.TopicName(), cm, off)
	return err
}

func (cmm *ControlChannelManager) AppendRescaleConfig(ctx context.Context,
	config map[string]uint8,
) error {
	cmm.currentEpoch += 1
	cm := txn_data.ControlMetadata{
		Config: config,
		Epoch:  cmm.currentEpoch,
	}
	err := cmm.appendToControlLog(ctx, &cm)
	return err
}

func (cmm *ControlChannelManager) TrackAndAppendKeyMapping(
	ctx context.Context,
	key interface{},
	keySerde commtypes.Serde,
	substreamId uint8,
	topic string,
) error {
	kBytes, err := keySerde.Encode(key)
	if err != nil {
		return err
	}
	cmm.kmMu.Lock()
	defer cmm.kmMu.Unlock()
	subs, ok := cmm.keyMappings[topic]
	if !ok {
		subs = skiplist.New(skiplist.Bytes)
		cmm.keyMappings[topic] = subs
	}
	subTmp, ok := subs.GetValue(kBytes)
	var sub map[uint8]struct{}
	if !ok {
		sub = make(map[uint8]struct{})
		subs.Set(kBytes, sub)
	} else {
		sub = subTmp.(map[uint8]struct{})
	}
	_, ok = sub[substreamId]
	if !ok {
		sub[substreamId] = struct{}{}
		cm := txn_data.ControlMetadata{
			Key:         kBytes,
			SubstreamId: substreamId,
			Topic:       topic,
			Epoch:       cmm.currentEpoch,
		}
		err = cmm.appendToControlLog(ctx, &cm)
	}
	return err
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
	err := cmm.appendToControlLog(ctx, &cm)
	return err
}

func (cmm *ControlChannelManager) updateKeyMapping(ctrlMeta *txn_data.ControlMetadata) {
	cmm.kmMu.Lock()
	subs, ok := cmm.keyMappings[ctrlMeta.Topic]
	if !ok {
		subs = skiplist.New(skiplist.Bytes)
		cmm.keyMappings[ctrlMeta.Topic] = subs
	}
	subTmp, ok := subs.GetValue(ctrlMeta.Key)
	var sub map[uint8]struct{}
	if !ok {
		sub = make(map[uint8]struct{})
		subs.Set(ctrlMeta.Key, sub)
	} else {
		sub = subTmp.(map[uint8]struct{})
	}
	_, ok = sub[ctrlMeta.SubstreamId]
	if !ok {
		sub[ctrlMeta.SubstreamId] = struct{}{}
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

		rawMsg, err := cmm.controlLog.ReadNext(ctx, 0)
		if err != nil {
			if common_errors.IsStreamEmptyError(err) {
				continue
			}
			output <- ControlChannelErr(err)
			break
		} else {
			_, valBytes, err := cmm.msgSerde.Decode(rawMsg.Payload)
			if err != nil {
				output <- ControlChannelErr(err)
				break
			}
			ctrlMetaTmp, err := cmm.controlMetaSerde.Decode(valBytes)
			if err != nil {
				output <- ControlChannelErr(err)
				break
			}
			ctrlMeta := ctrlMetaTmp.(txn_data.ControlMetadata)
			// debug.Fprintf(os.Stderr, "MonitorControlChannel: tp %s got %v, off: %x\n",
			// 	cmm.controlLog.TopicName(), ctrlMeta, rawMsg.LogSeqNum)
			if ctrlMeta.Key != nil && ctrlMeta.Topic != "" {
				cmm.updateKeyMapping(&ctrlMeta)
			} else {
				output <- ControlChannelVal(&ctrlMeta)
			}
		}
	}
}
