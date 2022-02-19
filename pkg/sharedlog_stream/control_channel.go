package sharedlog_stream

import (
	"context"
	"fmt"
	"sharedlog-stream/pkg/errors"
	"sharedlog-stream/pkg/hash"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"sync"

	"cs.utexas.edu/zjia/faas/types"
	"github.com/huandu/skiplist"
)

const (
	CONTROL_LOG_TOPIC_NAME = "__control_log"
)

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
	controlLog       *SharedLogStream

	kmMu        sync.Mutex
	keyMappings map[string]*skiplist.SkipList

	topicStreams map[string]*ShardedSharedLogStream
	output_topic string

	cHashMu *sync.RWMutex
	cHash   *hash.ConsistentHash

	funcName     string
	currentEpoch uint64
	instanceId   uint8
}

func NewControlChannelManager(env types.Environment,
	app_id string,
	serdeFormat commtypes.SerdeFormat,
) (*ControlChannelManager, error) {
	log, err := NewSharedLogStream(env, app_id+"_"+CONTROL_LOG_TOPIC_NAME, serdeFormat)
	if err != nil {
		return nil, err
	}
	cm := &ControlChannelManager{
		env:          env,
		controlLog:   log,
		currentEpoch: 0,
		topicStreams: make(map[string]*ShardedSharedLogStream),
		keyMappings:  make(map[string]*skiplist.SkipList),
	}
	if serdeFormat == commtypes.JSON {
		cm.controlMetaSerde = ControlMetadataJSONSerde{}
		cm.msgSerde = commtypes.MessageSerializedJSONSerde{}
	} else if serdeFormat == commtypes.MSGP {
		cm.controlMetaSerde = ControlMetadataMsgpSerde{}
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
		ctrlMeta := ctrlMetaTmp.(ControlMetadata)
		if ctrlMeta.Config == nil {
			cmm.updateKeyMapping(&ctrlMeta)
		}
	}
	return nil
}

func (cmm *ControlChannelManager) TrackStream(topicName string, stream *ShardedSharedLogStream) {
	cmm.topicStreams[topicName] = stream
}

func (cmm *ControlChannelManager) TrackConsistentHash(cHashMu *sync.RWMutex, cHash *hash.ConsistentHash) {
	cmm.cHash = cHash
	cmm.cHashMu = cHashMu
}

func (cmm *ControlChannelManager) TrackOutputTopic(output_topic string) {
	cmm.output_topic = output_topic
}

func (cmm *ControlChannelManager) appendToControlLog(ctx context.Context, cm *ControlMetadata) error {
	encoded, err := cmm.controlMetaSerde.Encode(cm)
	if err != nil {
		return err
	}
	msg_encoded, err := cmm.msgSerde.Encode(nil, encoded)
	if err != nil {
		return err
	}
	_, err = cmm.controlLog.Push(ctx, msg_encoded, 0, false)
	return err
}

func (cmm *ControlChannelManager) AppendRescaleConfig(ctx context.Context,
	config map[string]uint8,
) error {
	cmm.currentEpoch += 1
	cm := ControlMetadata{
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
		cm := ControlMetadata{
			Key:         kBytes,
			SubstreamId: substreamId,
			Topic:       topic,
			Epoch:       cmm.currentEpoch,
		}
		err = cmm.appendToControlLog(ctx, &cm)
	}
	return err
}

func (cmm *ControlChannelManager) updateKeyMapping(ctrlMeta *ControlMetadata) {
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
	dcancel context.CancelFunc,
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
				ctrlMeta := ctrlMetaTmp.(ControlMetadata)
				if ctrlMeta.Config != nil {
					// update to new epoch
					cmm.currentEpoch = ctrlMeta.Epoch
					numInstance := ctrlMeta.Config[cmm.funcName]
					if cmm.instanceId >= numInstance {
						// new config scales down. we need to exit
						dcancel()
						errc <- nil
						break
					}
					numSubstreams := ctrlMeta.Config[cmm.output_topic]
					if cmm.cHashMu != nil && cmm.cHash != nil {
						output_stream := cmm.topicStreams[cmm.output_topic]
						cur_subs := output_stream.numPartitions
						updateConsistentHash(cmm.cHashMu, cmm.cHash, cur_subs, numSubstreams)
					}
					for topic, stream := range cmm.topicStreams {
						numSubstreams, ok := ctrlMeta.Config[topic]
						if ok {
							err = stream.ScaleSubstreams(cmm.env, numSubstreams)
							if err != nil {
								errc <- err
								break
							}
						}
					}
				} else {
					cmm.updateKeyMapping(&ctrlMeta)
				}
			}
		}
	}
}
