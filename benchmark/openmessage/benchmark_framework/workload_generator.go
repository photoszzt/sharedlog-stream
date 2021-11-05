package benchmarkframework

type WorkloadGenerator struct {
	worker     Worker
	driverName string
	workload   Workload
}
