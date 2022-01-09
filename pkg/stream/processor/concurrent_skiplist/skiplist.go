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
	"math"
	"math/rand"
	"time"
)

const (
	// Suitable for math.Floor(math.Pow(math.E, 18)) == 65659969 elements in list
	DefaultMaxLevel    int     = 18
	DefaultProbability float64 = 1 / math.E
)

// Front returns the head node of the list.
func (list *SkipList) Front() *Element {
	return list.next[0]
}

// Set inserts a value in the list with the specified key, ordered by the key.
// If the key exists, it updates the value in the existing node.
// Returns a pointer to the new element.
// Locking is optimistic and happens only after searching.
func (list *SkipList) Set(key KeyT, value ValueT) *Element {
	list.mutex.Lock()
	defer list.mutex.Unlock()

	var element *Element
	prevs := list.getPrevElementNodes(key)

	if element = prevs[0].next[0]; element != nil && list.comparable.Compare(element.key, key) <= 0 {
		element.value = value
		return element
	}

	element = &Element{
		elementNode: elementNode{
			next: make([]*Element, list.randLevel()),
		},
		key:   key,
		value: value,
	}

	for i := range element.next {
		element.next[i] = prevs[i].next[i]
		prevs[i].next[i] = element
	}

	list.Length++
	return element
}

func (list *SkipList) SetIfAbsent(key KeyT, value ValueT) *Element {
	list.mutex.Lock()
	defer list.mutex.Unlock()
	var element *Element
	prevs := list.getPrevElementNodes(key)

	if element = prevs[0].next[0]; element != nil && list.comparable.Compare(element.key, key) <= 0 {
		return nil
	}

	element = &Element{
		elementNode: elementNode{
			next: make([]*Element, list.randLevel()),
		},
		key:   key,
		value: value,
	}

	for i := range element.next {
		element.next[i] = prevs[i].next[i]
		prevs[i].next[i] = element
	}

	list.Length++
	return element
}

func (list *SkipList) ComputeIfPresent(key KeyT, computeFunc func(*Element)) {
	list.mutex.Lock()
	defer list.mutex.Unlock()
	var element *Element
	prevs := list.getPrevElementNodes(key)

	if element = prevs[0].next[0]; element != nil && list.comparable.Compare(element.key, key) == 0 {
		computeFunc(element)
	}
}

// Get finds an element by key. It returns element pointer if found, nil if not found.
// Locking is optimistic and happens only after searching with a fast check for deletion after locking.
func (list *SkipList) Get(key KeyT) *Element {
	list.mutex.Lock()
	defer list.mutex.Unlock()

	return list.getWithLockHeld(key)
}

func (list *SkipList) getWithLockHeld(key KeyT) *Element {
	var prev *elementNode = &list.elementNode
	var next *Element

	for i := list.maxLevel - 1; i >= 0; i-- {
		next = prev.next[i]

		for next != nil && list.comparable.Compare(key, next.key) > 0 {
			prev = &next.elementNode
			next = next.next[i]
		}
	}

	if next != nil && list.comparable.Compare(next.key, key) <= 0 {
		return next
	}

	return nil
}

func (list *SkipList) findBeginNode(key KeyT) *Element {
	var prev *elementNode = &list.elementNode
	var next *Element

	for i := list.maxLevel - 1; i >= 0; i-- {
		next = prev.next[i]

		for next != nil && list.comparable.Compare(key, next.key) > 0 {
			prev = &next.elementNode
			next = next.next[i]
		}
	}

	return next
}

func (list *SkipList) findEndNode(key KeyT) *Element {
	var prev *elementNode = &list.elementNode
	var next *Element
	var curMaxWithinKey *Element

	for i := list.maxLevel - 1; i >= 0; i-- {
		next = prev.next[i]

		for next != nil && list.comparable.Compare(key, next.key) > 0 {
			curMaxWithinKey = next
			prev = &next.elementNode
			next = next.next[i]
		}
	}

	if next != nil && list.comparable.Compare(next.key, key) <= 0 {
		return next
	}

	return curMaxWithinKey
}

func (list *SkipList) IterateRange(startK KeyT, endK KeyT, iterFunc func(KeyT, ValueT) error) error {
	list.mutex.Lock()
	defer list.mutex.Unlock()

	beg := list.findBeginNode(startK)
	end := list.findEndNode(endK)

	for i := beg; i != nil && list.comparable.Compare(i.key, end.key) <= 0; i = i.Next() {
		err := iterFunc(i.key, i.value)
		if err != nil {
			return err
		}
	}
	return nil
}

// Remove deletes an element from the list.
// Returns removed element pointer if found, nil if not found.
// Locking is optimistic and happens only after searching with a fast check on adjacent nodes after locking.
func (list *SkipList) Remove(key KeyT) *Element {
	list.mutex.Lock()
	defer list.mutex.Unlock()
	return list.removeWithLockHeld(key)
}

func (list *SkipList) removeWithLockHeld(key KeyT) *Element {
	prevs := list.getPrevElementNodes(key)

	// found the element, remove it
	if element := prevs[0].next[0]; element != nil && list.comparable.Compare(element.key, key) <= 0 {
		for k, v := range element.next {
			prevs[k].next[k] = v
		}

		list.Length--
		return element
	}

	return nil
}

// Remove elements until key (key is not included)
func (list *SkipList) RemoveUntil(key KeyT) []*Element {
	list.mutex.Lock()
	defer list.mutex.Unlock()

	elements := make([]*Element, 0)
	e := list.Front()
	for e != nil {
		if list.comparable.Compare(e.key, key) < 0 {
			elements = append(elements, list.removeWithLockHeld(e.key))
		} else {
			break
		}
		e = list.Front()
	}
	return elements
}

// getPrevElementNodes is the private search mechanism that other functions use.
// Finds the previous nodes on each level relative to the current Element and
// caches them. This approach is similar to a "search finger" as described by Pugh:
// http://citeseerx.ist.psu.edu/viewdoc/summary?doi=10.1.1.17.524
func (list *SkipList) getPrevElementNodes(key KeyT) []*elementNode {
	var prev *elementNode = &list.elementNode
	var next *Element

	prevs := list.prevNodesCache

	for i := list.maxLevel - 1; i >= 0; i-- {
		next = prev.next[i]

		for next != nil && list.comparable.Compare(key, next.key) > 0 {
			prev = &next.elementNode
			next = next.next[i]
		}

		prevs[i] = prev
	}

	return prevs
}

// SetProbability changes the current P value of the list.
// It doesn't alter any existing data, only changes how future insert heights are calculated.
func (list *SkipList) SetProbability(newProbability float64) {
	list.probability = newProbability
	list.probTable = probabilityTable(list.probability, list.maxLevel)
}

func (list *SkipList) randLevel() (level int) {
	// Our random number source only has Int63(), so we have to produce a float64 from it
	// Reference: https://golang.org/src/math/rand/rand.go#L150
	r := float64(list.randSource.Int63()) / (1 << 63)

	level = 1
	for level < list.maxLevel && r < list.probTable[level] {
		level++
	}
	return
}

// probabilityTable calculates in advance the probability of a new node having a given level.
// probability is in [0, 1], MaxLevel is (0, 64]
// Returns a table of floating point probabilities that each level should be included during an insert.
func probabilityTable(probability float64, MaxLevel int) (table []float64) {
	for i := 1; i <= MaxLevel; i++ {
		prob := math.Pow(probability, float64(i-1))
		table = append(table, prob)
	}
	return table
}

// NewWithMaxLevel creates a new skip list with MaxLevel set to the provided number.
// maxLevel has to be int(math.Ceil(math.Log(N))) for DefaultProbability (where N is an upper bound on the
// number of elements in a skip list). See http://citeseerx.ist.psu.edu/viewdoc/summary?doi=10.1.1.17.524
// Returns a pointer to the new list.
func NewWithMaxLevel(maxLevel int, compare Comparable) *SkipList {
	if maxLevel < 1 || maxLevel > 64 {
		panic("maxLevel for a SkipList must be a positive integer <= 64")
	}

	return &SkipList{
		elementNode:    elementNode{next: make([]*Element, maxLevel)},
		prevNodesCache: make([]*elementNode, maxLevel),
		maxLevel:       maxLevel,
		randSource:     rand.New(rand.NewSource(time.Now().UnixNano())),
		probability:    DefaultProbability,
		probTable:      probabilityTable(DefaultProbability, maxLevel),
		comparable:     compare,
	}
}

// New creates a new skip list with default parameters. Returns a pointer to the new list.
func New(compare Comparable) *SkipList {
	return NewWithMaxLevel(DefaultMaxLevel, compare)
}
