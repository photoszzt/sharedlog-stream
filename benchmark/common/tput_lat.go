package common

import (
	"encoding/json"
	"fmt"
	"os"
	"path"
	"sharedlog-stream/pkg/debug"
	"sort"
	"strings"
)

type TimeSlice []int

func (t TimeSlice) Len() int {
	return len(t)
}

func (t TimeSlice) Less(i, j int) bool {
	return t[i] < t[j]
}

func (t TimeSlice) Swap(i, j int) {
	t[i], t[j] = t[j], t[i]
}

func (t TimeSlice) P(percent float64) int {
	return t[int(float64(t.Len())*percent+0.5)-1]
}

type TimeSlice64 []int64

func (t TimeSlice64) Len() int {
	return len(t)
}

func (t TimeSlice64) Less(i, j int) bool {
	return t[i] < t[j]
}

func (t TimeSlice64) Swap(i, j int) {
	t[i], t[j] = t[j], t[i]
}

func (t TimeSlice64) P(percent float64) int64 {
	return t[int(float64(t.Len())*percent+0.5)-1]
}

type benchStats struct {
	Latencies map[string][]int
	Consumed  map[string]uint64
	Duration  float64
}

func ProcessThroughputLat(name string, stat_dir string, latencies map[string][]int,
	consumed map[string]uint64, duration float64,
	num map[string]uint64, endToEnd *float64,
) {
	path := path.Join(stat_dir, fmt.Sprintf("%s.json", name))
	if err := os.MkdirAll(stat_dir, 0750); err != nil {
		panic(fmt.Sprintf("Fail to create stat dir: %v", err))
	}
	st := benchStats{
		Latencies: latencies,
		Consumed:  consumed,
		Duration:  duration,
	}
	stBytes, err := json.Marshal(st)
	if err != nil {
		panic(fmt.Sprintf("fail to marshal stats: %v", err))
	}
	err = os.WriteFile(path, stBytes, 0666)
	if err != nil {
		panic(fmt.Sprintf("fail to write stats to file: %v", err))
	}
	for n, lat_arr := range latencies {
		if len(lat_arr) != 0 {
			ts := TimeSlice(lat_arr)
			sort.Sort(ts)
			if n != "eventTimeLatency" {
				sumTime := float64(0)
				for _, lat := range lat_arr {
					sumTime += float64(lat) / 1000000.0
				}
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
				if n == "src" || strings.Contains(name, "source") || strings.Contains(n, "Src") || n == "sink" || strings.Contains(n, "Sink") {
					num[n] += processed
				}
				if n == "e2e" {
					if *endToEnd < sumTime {
						*endToEnd = sumTime
					}
				}
				tput := float64(processed) / sumTime
				fmt.Fprintf(os.Stderr, "sum of %s time: %v ", n, sumTime)
				fmt.Fprintf(os.Stderr, "processed: %v, throughput: (event/s) %v, p50: %d us, p90: %d us, p99: %d us\n",
					processed, tput, ts.P(0.5), ts.P(0.9), ts.P(0.99))
			} else {
				fmt.Fprintf(os.Stderr, "%s, p50: %d us, p90: %d us, p99: %d\n", n, ts.P(0.5), ts.P(0.9), ts.P(0.99))
			}
		}
	}
	fmt.Fprintf(os.Stderr, "%s duration: %v\n\n", name, duration)
}
