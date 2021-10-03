package spike_detection

import (
	"context"

	"cs.utexas.edu/zjia/faas/types"
)

type spikeDetectionGroupBy struct {
	env types.Environment
}

func NewSpikeDetectionGroupBy(env types.Environment) types.FuncHandler {
	return &spikeDetectionGroupBy{
		env: env,
	}
}

func (h *spikeDetectionGroupBy) Call(ctx context.Context, input []byte) ([]byte, error) {
	return nil, nil
}
