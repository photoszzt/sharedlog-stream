package common

type QueryInput struct {
	Duration        uint32 `json:"duration"`
	InputTopicName  string `json:"input_topic_name"`
	OutputTopicName string `json:"output_topic_name"`
	SerdeFormat     uint8  `json:"serde_format"`
	NumInPartition  uint16 `json:"numInPartition"`
	NumOutPartition uint16 `json:"numOutPartition"`
	PartNum         uint16 `json:"PartNum"`
}

type FnOutput struct {
	Success   bool    `json:"success"`
	Message   string  `json:"message"`
	Duration  float64 `json:"duration"`
	Latencies []int   `json:"latencies"`
}

type SourceParam struct {
	TopicName   string `json:"topicName"`
	FileName    string `json:"fname"`
	Duration    uint32 `json:"duration"` // in sec
	SerdeFormat uint8  `json:"serdeFormat"`
	NumEvents   uint32 `json:"numEvents"`
}
