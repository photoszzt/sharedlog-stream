package benchmarkframework

type WorkloadGenerator struct {
	Worker     Worker
	DriverName string
	Workload   Workload
}
