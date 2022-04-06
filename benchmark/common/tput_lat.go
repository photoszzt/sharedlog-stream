package common

import (
	"fmt"
	"os"
	"sharedlog-stream/pkg/debug"
	"sort"
	"strings"
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

func ProcessThroughputLat(name string, latencies map[string][]int,
	consumed map[string]uint64, duration float64,
	num *uint64, endToEnd *float64,
) {
	fmt.Fprintf(os.Stderr, "process %s\n", name)
	for n, lat_arr := range latencies {
		if len(lat_arr) != 0 {
			sumTime := float64(0)
			ts := timeSlice(lat_arr)
			sort.Sort(ts)
			for _, lat := range lat_arr {
				sumTime += float64(lat) / 1000.0
			}
			sumTime = sumTime / 1000.0 // convert to sec
			processed := uint64(len(lat_arr))
			var ok bool
			if consumed != nil {
				processed, ok = consumed[n]
				if !ok {
					debug.Fprintf(os.Stderr, "does not find %s in consumed\n", n)
					processed = uint64(len(lat_arr))
				}
			} else {
				debug.Fprint(os.Stderr, "consumed is empty")
			}
			if strings.Contains(n, "src") || (strings.Contains(name, "source") && n == "e2e") {
				fmt.Fprintf(os.Stderr, "collect %s for %s\n", n, name)
				*num += processed
				if sumTime > *endToEnd {
					*endToEnd = sumTime
				}
			}
			tput := float64(processed) / sumTime
			fmt.Fprintf(os.Stderr, "sum of %s time: %v ", n, sumTime)
			fmt.Fprintf(os.Stderr, "processed: %v, throughput: (event/s) %v, p50: %d us, p90: %d us, p99: %d us\n",
				processed, tput, ts.p(0.5), ts.p(0.9), ts.p(0.99))
		}
	}
	fmt.Fprintf(os.Stderr, "%s duration: %v\n\n", name, duration)
}
