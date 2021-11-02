package worker

type Topic struct {
	Name       string
	Partitions uint32
}

func NewTopic(name string, partitions uint32) *Topic {
	return &Topic{
		Name:       name,
		Partitions: partitions,
	}
}
