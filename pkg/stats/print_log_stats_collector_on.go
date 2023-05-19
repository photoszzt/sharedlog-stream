//go:build printstats
// +build printstats

package stats

import (
	"fmt"
	"os"
)

func (c *PrintLogStatsCollector[E]) AddSample(sample E) {
	if len(c.data) >= cap(c.data) {
		fmt.Fprintf(os.Stderr, "%s: %v\n", c.tag, c.data)
		c.data = make([]E, 0, cap(c.data))
	}
	c.data = append(c.data, sample)
}