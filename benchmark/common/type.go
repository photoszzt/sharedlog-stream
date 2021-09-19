package common

type SerdeFormat uint8

const (
	JSON SerdeFormat = 0
	MSGP SerdeFormat = 1
)

type QueryInput struct {
	Duration        uint32 `json:"duration"`
	InputTopicName  string `json:"input_topic_name"`
	OutputTopicName string `json:"output_topic_name"`
	SerdeFormat     uint8  `json:"serde_format"`
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
