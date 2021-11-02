package benchmarkframework

type WorkloadGenerator struct {
	driverName string
	workload   Workload
	worker     Worker
}
