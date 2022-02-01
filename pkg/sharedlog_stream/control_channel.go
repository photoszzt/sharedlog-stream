package sharedlog_stream

import (
	"context"
	"fmt"
	"sharedlog-stream/pkg/stream/processor/commtypes"

	"cs.utexas.edu/zjia/faas/types"
)

const (
	CONTROL_LOG_TOPIC_NAME = "__control_log"
)

type ControlChannelManager struct {
	controlLog       *SharedLogStream
	controlMetaSerde commtypes.Serde
	msgSerde         commtypes.MsgSerde
	epoch            uint32
}

func NewControlChannelManager(env types.Environment,
	app_id string,
	serdeFormat commtypes.SerdeFormat,
) (*ControlChannelManager, error) {
	log := NewSharedLogStream(env, app_id+"_"+CONTROL_LOG_TOPIC_NAME)
	cm := &ControlChannelManager{
		controlLog: log,
		epoch:      0,
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

func (cmm *ControlChannelManager) Rescale(ctx context.Context, stages map[string]uint8) error {
	cmm.epoch += 1
	cm := ControlMetadata{
		Stages: stages,
		Epoch:  cmm.epoch,
	}
	err := cmm.appendToControlLog(ctx, &cm)
	return err
}

func (cmm *ControlChannelManager) AppendKeyMapping(
	ctx context.Context,
	key interface{},
	keySerde commtypes.Serde,
	instanceId uint8,
) error {
	kBytes, err := keySerde.Encode(key)
	if err != nil {
		return err
	}
	cm := ControlMetadata{
		Key:        kBytes,
		InstanceId: instanceId,
		Epoch:      cmm.epoch,
	}
	err = cmm.appendToControlLog(ctx, &cm)
	return err
}

func (cmm *ControlChannelManager) MonitorControlChannel(
	ctx context.Context,
	quit chan struct{},
	errc chan error,
) {
	for {
		select {
		case <-quit:
			return
		default:
		}

	}
}
