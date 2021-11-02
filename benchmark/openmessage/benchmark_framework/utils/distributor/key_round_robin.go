package distributor

type KeyRoundRobin struct {
	BaseKeyDistributor
	currentIdx uint32
}

func (kd *KeyRoundRobin) Next() string {
	kd.currentIdx += 1
	if kd.currentIdx >= kd.GetLength() {
		kd.currentIdx = 0
	}
	return kd.Get(kd.currentIdx)
}
