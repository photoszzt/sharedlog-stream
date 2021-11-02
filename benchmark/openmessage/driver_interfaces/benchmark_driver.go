package driverinterfaces

type BenchmarkDriver interface {
	Init(configFile string)
	GetTopicNamePrefix() string
	CreateTopic(topic string, partitions uint32)
	NotifyTopicCreation(topic string, partitions uint32)
	CreateProducer(topic string)
	CreateConsumer(topic string, subscriptionName string, partition uint32, consumerCallback ConsumerCallback)
}
