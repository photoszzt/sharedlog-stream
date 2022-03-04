package debug

import (
	"fmt"
	"os"
)

func PrintByteSlice(data []byte) {
	for i := 0; i < len(data); i++ {
		fmt.Fprintf(os.Stderr, "%x ", data[i])
	}
	fmt.Fprintf(os.Stderr, "\n")
}
