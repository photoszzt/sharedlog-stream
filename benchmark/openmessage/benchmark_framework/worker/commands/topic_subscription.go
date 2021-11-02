package commands

type TopicSubscription struct {
	Topic        string
	Subscription string
	Partition    uint32
}

func NewTopicSubscription(topic string, subscription string, partition uint32) *TopicSubscription {
	return &TopicSubscription{
		Topic:        topic,
		Subscription: subscription,
		Partition:    partition,
	}
}
