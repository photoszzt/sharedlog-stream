// Copyright 2013 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package genericlist_test

import (
	"fmt"
	"sharedlog-stream/pkg/data_structure/genericlist"
)

func Example() {
	// Create a new list and put some numbers in it.
	l := genericlist.New[int]()
	e4 := l.PushBack(4)
	e1 := l.PushFront(1)
	l.InsertBefore(3, e4)
	l.InsertAfter(2, e1)

	// Iterate through list and print its contents.
	for e := l.Front(); e != nil; e = e.Next() {
		fmt.Println(e.Value)
	}

	// Output:
	// 1
	// 2
	// 3
	// 4
}
