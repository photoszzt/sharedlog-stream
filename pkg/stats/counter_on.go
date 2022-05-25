//go:build stats
// +build stats

package stats

import (
	"fmt"
	"os"
	"sync/atomic"
)

func (c *Counter) Tick(count uint32) {
	c.count += uint64(count)
}

func (c *Counter) Report() {
	fmt.Fprintf(os.Stderr, "{%s_count: %d}\n", c.tag, c.count)
}

func (c *AtomicCounter) Tick(count uint32) {
	atomic.AddUint64(&c.count, uint64(count))
}
