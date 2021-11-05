package driverinterfaces

type BenchmarkDriver interface {
	Init(configFile string) error
	GetTopicNamePrefix() string
	CreateTopic(topic string, partitions uint32)
	NotifyTopicCreation(topic string, partitions uint32)
	CreateProducer(topic string)
	CreateConsumer(topic string, subscriptionName string, partition uint32, consumerCallback ConsumerCallback)
}

type BenchmarkProducer interface {
	SendAsync(key string, payload []byte)
}

type ConsumerCallback interface {
	MessageReceived(payload []byte, publishTimestamp uint64)
}

type BenchmarkConsumer interface{}
