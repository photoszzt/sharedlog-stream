package commtypes

import (
	"fmt"

	"cs.utexas.edu/zjia/faas/types"
)

var EmptyProducerId = ProducerId{TaskEpoch: 0, TaskId: 0}

type ProducerId struct {
	TaskId    uint64
	TaskEpoch uint16
}

var _ = fmt.Stringer(&ProducerId{})
var _ = fmt.GoStringer(&ProducerId{})

func (p *ProducerId) String() string {
	return fmt.Sprintf("ProducerId: {TaskId: %#x, TaskEpoch: %#x}",
		p.TaskId, p.TaskEpoch)
}

func (p *ProducerId) GoString() string {
	return fmt.Sprintf("ProducerId: {TaskId: %#x, TaskEpoch: %#x}",
		p.TaskId, p.TaskEpoch)
}

func NewProducerId() ProducerId {
	return ProducerId{
		TaskId:    0,
		TaskEpoch: 0,
	}
}

func (tg *ProducerId) GetCurrentEpoch() uint16 {
	return tg.TaskEpoch
}

func (tg *ProducerId) GetCurrentTaskId() uint64 {
	return tg.TaskId
}

func (tg *ProducerId) GetProducerId() ProducerId {
	return ProducerId{
		TaskId:    tg.TaskId,
		TaskEpoch: tg.TaskEpoch,
	}
}
func (tg *ProducerId) InitTaskId(env types.Environment) {
	for tg.TaskId == 0 {
		tg.TaskId = env.GenerateUniqueID()
	}
}
