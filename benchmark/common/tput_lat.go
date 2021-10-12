package common

import (
	"fmt"
	"os"
)

func ProcessThroughputLat(name string, latencies []int, duration float64) {
	sumTime := float64(0)
	for _, lat := range latencies {
		sumTime += float64(lat) / 1000.0
	}
	sumTime = sumTime / 1000.0 // convert to sec
	tput := float64(len(latencies)) / sumTime
	fmt.Fprintf(os.Stdout, "%s duration: %v ", name, duration)
	fmt.Fprintf(os.Stdout, "sum of iter time: %v ", sumTime)
	fmt.Fprintf(os.Stdout, "processed: %v, throughput: (event/s) %v\n", len(latencies), tput)
}
