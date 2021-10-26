package common

import (
	"fmt"
	"os"
	"sort"
)

type timeSlice []int

func (t timeSlice) Len() int {
	return len(t)
}

func (t timeSlice) Less(i, j int) bool {
	return t[i] < t[j]
}

func (t timeSlice) Swap(i, j int) {
	t[i], t[j] = t[j], t[i]
}

func (t timeSlice) p(percent float64) int {
	return t[int(float64(t.Len())*percent+0.5)-1]
}

func ProcessThroughputLat(name string, latencies map[string][]int, duration float64) {

	for n, lat_arr := range latencies {
		sumTime := float64(0)
		ts := timeSlice(lat_arr)
		sort.Sort(ts)
		for _, lat := range lat_arr {
			sumTime += float64(lat) / 1000.0
		}
		sumTime = sumTime / 1000.0 // convert to sec
		tput := float64(len(lat_arr)) / sumTime
		fmt.Fprintf(os.Stdout, "sum of %s time: %v ", n, sumTime)
		fmt.Fprintf(os.Stdout, "processed: %v, throughput: (event/s) %v, p50: %d us, p90: %d us\n", len(lat_arr), tput, ts.p(0.5), ts.p(0.9))
	}
	fmt.Fprintf(os.Stdout, "%s duration: %v\n\n", name, duration)
}
