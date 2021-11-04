package common

type QueryInput struct {
	OutputTopicName string `json:"output_topic_name"`
	InputTopicName  string `json:"input_topic_name"`
	Duration        uint32 `json:"duration"`
	SerdeFormat     uint8  `json:"serde_format"`
	NumInPartition  uint8  `json:"numInPartition"`
	NumOutPartition uint8  `json:"numOutPartition"`
	ParNum          uint8  `json:"ParNum"`
}

type FnOutput struct {
	Latencies map[string][]int `json:"latencies"`
	Message   string           `json:"message"`
	Duration  float64          `json:"duration"`
	Success   bool             `json:"success"`
}

type SourceParam struct {
	TopicName   string `json:"topicName"`
	FileName    string `json:"fname"`
	Duration    uint32 `json:"duration"` // in sec
	SerdeFormat uint8  `json:"serdeFormat"`
	NumEvents   uint32 `json:"numEvents"`
}
