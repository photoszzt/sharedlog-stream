package transaction

import (
	"context"
	"fmt"
	"os"
	"sharedlog-stream/pkg/debug"
	"sharedlog-stream/pkg/errors"
	"sharedlog-stream/pkg/hash"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"sharedlog-stream/pkg/txn_data"
	"sync"

	"cs.utexas.edu/zjia/faas/types"
	"github.com/huandu/skiplist"
)

const (
	CONTROL_LOG_TOPIC_NAME = "__control_log"
)

/*
func updateConsistentHash(cHashMu *sync.RWMutex, cHash *hash.ConsistentHash, curNumPar uint8, newNumPar uint8) {
	cHashMu.Lock()
	defer cHashMu.Unlock()
	if curNumPar > newNumPar {
		for i := newNumPar; i < curNumPar; i++ {
			cHash.Remove(i)
		}
	}
	if curNumPar < newNumPar {
		for i := curNumPar; i < newNumPar; i++ {
			cHash.Add(i)
		}
	}
}
*/

func SetupConsistentHash(cHashMu *sync.RWMutex, cHash *hash.ConsistentHash, numPartition uint8) {
	cHashMu.Lock()
	defer cHashMu.Unlock()
	for i := uint8(0); i < numPartition; i++ {
		cHash.Add(i)
	}
}

type ControlChannelManager struct {
	env              types.Environment
	controlMetaSerde commtypes.Serde
	msgSerde         commtypes.MsgSerde
	controlLog       *sharedlog_stream.SharedLogStream

	kmMu sync.Mutex
	// topic -> (key -> set of substreamid)
	keyMappings map[string]*skiplist.SkipList

	topicStreams map[string]*sharedlog_stream.ShardedSharedLogStream
	output_topic string

	cHashMu *sync.RWMutex
	cHash   *hash.ConsistentHash

	funcName     string
	currentEpoch uint64
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
		env:          env,
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
	_, rawMsgs, err := cmm.controlLog.ReadNext(ctx, 0)
	if err != nil {
		if errors.IsStreamEmptyError(err) {
			return nil
		}
	}
	for _, rawMsg := range rawMsgs {
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
	return nil
}

func (cmm *ControlChannelManager) TrackStream(topicName string, stream *sharedlog_stream.ShardedSharedLogStream) {
	cmm.topicStreams[topicName] = stream
}

func (cmm *ControlChannelManager) TrackConsistentHash(cHashMu *sync.RWMutex, cHash *hash.ConsistentHash) {
	cmm.cHash = cHash
	cmm.cHashMu = cHashMu
}

func (cmm *ControlChannelManager) TrackOutputTopic(output_topic string) {
	cmm.output_topic = output_topic
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
	off, err := cmm.controlLog.Push(ctx, msg_encoded, 0, false, false)
	debug.Fprintf(os.Stderr, "appendToControlLog: tp %s %v, off %x\n",
		cmm.controlLog.TopicName(), cm, off)
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

func (cmm *ControlChannelManager) MonitorControlChannel(
	ctx context.Context,
	quit chan struct{},
	errc chan error,
	meta chan txn_data.ControlMetadata,
) {
	for {
		select {
		case <-quit:
			return
		default:
		}

		_, rawMsgs, err := cmm.controlLog.ReadNext(ctx, 0)
		if err != nil {
			if errors.IsStreamEmptyError(err) {
				continue
			}
			errc <- err
			break
		} else {
			for _, rawMsg := range rawMsgs {
				_, valBytes, err := cmm.msgSerde.Decode(rawMsg.Payload)
				if err != nil {
					errc <- err
					break
				}
				ctrlMetaTmp, err := cmm.controlMetaSerde.Decode(valBytes)
				if err != nil {
					errc <- err
					break
				}
				ctrlMeta := ctrlMetaTmp.(txn_data.ControlMetadata)
				// debug.Fprintf(os.Stderr, "MonitorControlChannel: tp %s got %v, off: %x\n",
				// 	cmm.controlLog.TopicName(), ctrlMeta, rawMsg.LogSeqNum)
				if ctrlMeta.Key != nil && ctrlMeta.Topic != "" {
					cmm.updateKeyMapping(&ctrlMeta)
				} else {
					meta <- ctrlMeta
				}
			}
		}
	}
}
