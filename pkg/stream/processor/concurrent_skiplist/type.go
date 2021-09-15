// MIT License
//
// Copyright (c) 2017 sean
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

// skiplist adopt from https://github.com/sean-public/fast-skiplist.git
package concurrent_skiplist

import (
	"math/rand"
	"sync"
)

// template type SkipList(ValueT)
// template type ConcurrentSkiplistMap(KeyT, ValueT)

type KeyT interface{}

// ValueT is a generic value type of the element
type ValueT interface{}

type elementNode struct {
	next []*Element
}

type Element struct {
	elementNode
	key   KeyT
	value ValueT
}

// Key allows retrieval of the key for a given Element
func (e *Element) Key() KeyT {
	return e.key
}

// Value allows retrieval of the value for a given Element
func (e *Element) Value() ValueT {
	return e.value
}

// Next returns the following Element or nil if we're at the end of the list.
// Only operates on the bottom level of the skip list (a fully linked list).
func (element *Element) Next() *Element {
	return element.next[0]
}

type SkipList struct {
	elementNode
	maxLevel       int
	Length         int
	randSource     rand.Source
	probability    float64
	probTable      []float64
	mutex          sync.RWMutex
	prevNodesCache []*elementNode
	comparable     Comparable
}

type Comparable interface {
	Compare(lhs, rhs interface{}) int
}

type CompareFunc func(lhs, rhs interface{}) int

func (f CompareFunc) Compare(lhs, rhs interface{}) int {
	return f(lhs, rhs)
}
