package benchmarkframework

//go:generate gotemplate "github.com/igrmk/treemap" "LatencyQuantileMap(float64, float64)"

type TestResult struct {
	Workload string
	Driver   string

	PublishRate []float64
	ConsumeRate []float64
	Backlog     []uint64

	PublishLatencyAvg     []float64
	PublishLatency50pct   []float64
	PublishLatency75pct   []float64
	PublishLatency95pct   []float64
	PublishLatency99pct   []float64
	PublishLatency999pct  []float64
	PublishLatency9999pct []float64
	PublishLatencyMax     []float64

	AggregatedPublishLatencyAvg     float64
	AggregatedPublishLatency50pct   float64
	AggregatedPublishLatency75pct   float64
	AggregatedPublishLatency95pct   float64
	AggregatedPublishLatency99pct   float64
	AggregatedPublishLatency999pct  float64
	AggregatedPublishLatency9999pct float64
	AggregatedPublishLatencyMax     float64

	AggregatePublishLatencyQuantiles LatencyQuantileMap

	EndtoEndLatencyAvg     []float64
	EndtoEndLatency50pct   []float64
	EndtoEndLatency75pct   []float64
	EndtoEndLatency95pct   []float64
	EndtoEndLatency99pct   []float64
	EndtoEndLatency999pct  []float64
	EndtoEndLatency9999pct []float64
	EndtoEndLatencyMax     []float64

	AggregatedEndToEndLatencyQuantiles LatencyQuantileMap

	AggregatedEndToEndLatencyAvg     float64
	AggregatedEndToEndLatency50pct   float64
	AggregatedEndToEndLatency75pct   float64
	AggregatedEndToEndLatency95pct   float64
	AggregatedEndToEndLatency99pct   float64
	AggregatedEndToEndLatency999pct  float64
	AggregatedEndToEndLatency9999pct float64
	AggregatedEndToEndLatencyMax     float64
}

func NewTestResult() *TestResult {
	return &TestResult{
		PublishRate:           make([]float64, 0, 8),
		ConsumeRate:           make([]float64, 0, 8),
		Backlog:               make([]uint64, 0, 8),
		PublishLatencyAvg:     make([]float64, 0, 8),
		PublishLatency50pct:   make([]float64, 0, 8),
		PublishLatency75pct:   make([]float64, 0, 8),
		PublishLatency95pct:   make([]float64, 0, 8),
		PublishLatency99pct:   make([]float64, 0, 8),
		PublishLatency999pct:  make([]float64, 0, 8),
		PublishLatency9999pct: make([]float64, 0, 8),
		PublishLatencyMax:     make([]float64, 0, 8),
		AggregatePublishLatencyQuantiles: *NewLatencyQuantileMap(func(a, b float64) bool {
			return a < b
		}),
		EndtoEndLatencyAvg:     make([]float64, 0, 8),
		EndtoEndLatency50pct:   make([]float64, 0, 8),
		EndtoEndLatency75pct:   make([]float64, 0, 8),
		EndtoEndLatency95pct:   make([]float64, 0, 8),
		EndtoEndLatency99pct:   make([]float64, 0, 8),
		EndtoEndLatency999pct:  make([]float64, 0, 8),
		EndtoEndLatency9999pct: make([]float64, 0, 8),
		EndtoEndLatencyMax:     make([]float64, 0, 8),

		AggregatedEndToEndLatencyQuantiles: *NewLatencyQuantileMap(func(a, b float64) bool {
			return a < b
		}),
	}
}
