package distributor

import "time"

type RandomNano struct {
	BaseKeyDistributor
}

func (kd *RandomNano) Next() string {
	randomIdx := time.Now().UnixNano() % int64(kd.GetLength())
	if randomIdx < 0 {
		randomIdx = -randomIdx
	}
	return kd.Get(uint32(randomIdx))
}
