package sharedlog_stream

import (
	"context"
	"fmt"
	"sharedlog-stream/pkg/errors"
	"sharedlog-stream/pkg/hash"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"sync"

	"cs.utexas.edu/zjia/faas/types"
)

const (
	CONTROL_LOG_TOPIC_NAME = "__control_log"
)

func UpdateConsistentHash(cHashMu *sync.RWMutex, cHash *hash.ConsistentHash, numPartition uint8) {
	cHashMu.Lock()
	defer cHashMu.Unlock()
	*cHash = *hash.NewConsistentHash()
	for i := uint8(0); i < numPartition; i++ {
		(*cHash).Add(i)
	}
}

func SetupConsistentHash(cHashMu *sync.RWMutex, cHash *hash.ConsistentHash, numPartition uint8) {
	cHashMu.Lock()
	defer cHashMu.Unlock()
	for i := uint8(0); i < numPartition; i++ {
		(*cHash).Add(i)
	}
}

type ControlChannelManager struct {
	env              types.Environment
	controlMetaSerde commtypes.Serde
	msgSerde         commtypes.MsgSerde
	controlLog       *SharedLogStream
	keyMappings      map[string]map[interface{}]map[uint8]struct{}
	topicStreams     map[string]*ShardedSharedLogStream
	output_topic     string
	cHashMu          *sync.RWMutex
	cHash            *hash.ConsistentHash
	funcName         string
	currentEpoch     uint64
	instanceId       uint8
}

func NewControlChannelManager(env types.Environment,
	app_id string,
	serdeFormat commtypes.SerdeFormat,
) (*ControlChannelManager, error) {
	log := NewSharedLogStream(env, app_id+"_"+CONTROL_LOG_TOPIC_NAME)
	cm := &ControlChannelManager{
		env:          env,
		controlLog:   log,
		currentEpoch: 0,
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

func (cmm *ControlChannelManager) AppendKeyMapping(
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
	cm := ControlMetadata{
		Key:         kBytes,
		SubstreamId: substreamId,
		Topic:       topic,
		Epoch:       cmm.currentEpoch,
	}
	err = cmm.appendToControlLog(ctx, &cm)
	return err
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
						UpdateConsistentHash(cmm.cHashMu, cmm.cHash, numSubstreams)
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
					keySubs := cmm.keyMappings[ctrlMeta.Topic]
					subs := keySubs[ctrlMeta.Key]
					subs[ctrlMeta.SubstreamId] = struct{}{}
				}
			}
		}
	}
}
