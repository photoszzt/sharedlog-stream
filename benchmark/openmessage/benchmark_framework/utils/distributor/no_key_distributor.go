package distributor

type NoKeyDistributor struct {
	BaseKeyDistributor
}

func (kd *NoKeyDistributor) Next() string {
	return ""
}
