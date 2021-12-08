//go:build debug
// +build debug

package debug

import (
	"fmt"
	"os"
)

// Assert will panic with msg if cond is false.
//
// msg must be a string, func() string or fmt.Stringer.
func Assert(cond bool, msg interface{}) {
	fmt.Fprintln(os.Stderr, "assert enable\n")
	if !cond {
		panic(getStringValue(msg))
	}
}
