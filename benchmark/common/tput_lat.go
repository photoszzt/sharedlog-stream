package common

import (
	"fmt"
	"os"
)

func ProcessThroughputLat(name string, latencies map[string][]int, duration float64) {

	for name, lat_arr := range latencies {
		sumTime := float64(0)
		for _, lat := range lat_arr {
			sumTime += float64(lat) / 1000.0
		}
		sumTime = sumTime / 1000.0 // convert to sec
		tput := float64(len(lat_arr)) / sumTime
		fmt.Fprintf(os.Stdout, "sum of %s time: %v ", name, sumTime)
		fmt.Fprintf(os.Stdout, "processed: %v, throughput: (event/s) %v\n", len(lat_arr), tput)
	}
	fmt.Fprintf(os.Stdout, "%s duration: %v ", name, duration)
}
