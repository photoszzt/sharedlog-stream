package driverinterfaces

type BenchmarkProducer interface {
	SendAsync(key string, payload []byte)
}
