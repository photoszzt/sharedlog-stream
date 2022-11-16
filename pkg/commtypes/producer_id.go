package commtypes

import (
	"fmt"

	"cs.utexas.edu/zjia/faas/types"
)

var EmptyProducerId = ProducerId{TaskEpoch: 0, TaskId: 0, TransactionID: 0}

type ProducerId struct {
	TaskId        uint64
	TransactionID uint64
	TaskEpoch     uint16
}

var _ = fmt.Stringer(&ProducerId{})

func (p *ProducerId) String() string {
	return fmt.Sprintf("ProducerId: {TaskId: 0x%x, TransactionId: 0x%x, TaskEpoch: 0x%x}",
		p.TaskId, p.TransactionID, p.TaskEpoch)
}

func NewProducerId() ProducerId {
	return ProducerId{
		TaskId:        0,
		TaskEpoch:     0,
		TransactionID: 0,
	}
}

func (tg *ProducerId) GetCurrentEpoch() uint16 {
	return tg.TaskEpoch
}

func (tg *ProducerId) GetCurrentTaskId() uint64 {
	return tg.TaskId
}

func (tg *ProducerId) GetTransactionID() uint64 {
	return tg.TransactionID
}

func (tg *ProducerId) GetProducerId() ProducerId {
	return ProducerId{
		TaskId:        tg.TaskId,
		TaskEpoch:     tg.TaskEpoch,
		TransactionID: tg.TransactionID,
	}
}
func (tg *ProducerId) InitTaskId(env types.Environment) {
	for tg.TaskId == 0 {
		tg.TaskId = env.GenerateUniqueID()
	}
}
