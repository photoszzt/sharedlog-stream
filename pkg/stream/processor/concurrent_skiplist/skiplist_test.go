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
	"fmt"
	"sync"
	"testing"
	"unsafe"
)

var benchList *SkipList

func init() {
	// Initialize a big SkipList for the Get() benchmark
	benchList = New(CompareFunc(func(lhs interface{}, rhs interface{}) int {
		l := lhs.(float64)
		r := lhs.(float64)
		if l < r {
			return -1
		} else if l == r {
			return 0
		} else {
			return 1
		}
	}))

	for i := 0; i <= 10000000; i++ {
		benchList.Set(float64(i), [1]byte{})
	}

	// Display the sizes of our basic structs
	var sl SkipList
	var el Element
	fmt.Printf("Structure sizes: SkipList is %v, Element is %v bytes\n", unsafe.Sizeof(sl), unsafe.Sizeof(el))
}

func checkSanity(list *SkipList, t *testing.T) {
	// each level must be correctly ordered
	for k, v := range list.next {
		//t.Log("Level", k)

		if v == nil {
			continue
		}

		if k > len(v.next) {
			t.Fatal("first node's level must be no less than current level")
		}

		next := v
		cnt := 1

		for next.next[k] != nil {
			if !(list.comparable.Compare(next.next[k].key, next.key) >= 0) {
				t.Fatalf("next key value must be greater than prev key value. [next:%v] [prev:%v]", next.next[k].key, next.key)
			}

			if k > len(next.next) {
				t.Fatalf("node's level must be no less than current level. [cur:%v] [node:%v]", k, next.next)
			}

			next = next.next[k]
			cnt++
		}

		if k == 0 {
			if cnt != list.Length {
				t.Fatalf("list len must match the level 0 nodes count. [cur:%v] [level0:%v]", cnt, list.Length)
			}
		}
	}
}

func TestBasicIntCRUD(t *testing.T) {

	list := New(CompareFunc(func(lhs interface{}, rhs interface{}) int {
		l := lhs.(int)
		r := rhs.(int)
		if l < r {
			return -1
		} else if l == r {
			return 0
		} else {
			return 1
		}
	}))

	list.Set(10, 1)
	list.Set(60, 2)
	list.Set(30, 3)
	list.Set(20, 4)
	list.Set(90, 5)
	checkSanity(list, t)

	list.Set(30, 9)
	checkSanity(list, t)

	list.Remove(0)
	list.Remove(20)
	checkSanity(list, t)

	v1 := list.Get(10)
	v2 := list.Get(60)
	v3 := list.Get(30)
	v4 := list.Get(20)
	v5 := list.Get(90)
	v6 := list.Get(0)

	if v1 == nil || v1.value.(int) != 1 || v1.key != 10 {
		t.Fatal(`wrong "10" value (expected "1")`, v1)
	}

	if v2 == nil || v2.value.(int) != 2 {
		t.Fatal(`wrong "60" value (expected "2")`)
	}

	if v3 == nil || v3.value.(int) != 9 {
		t.Fatal(`wrong "30" value (expected "9")`)
	}

	if v4 != nil {
		t.Fatal(`found value for key "20", which should have been deleted`)
	}

	if v5 == nil || v5.value.(int) != 5 {
		t.Fatal(`wrong "90" value`)
	}

	if v6 != nil {
		t.Fatal(`found value for key "0", which should have been deleted`)
	}
}

func TestChangeLevel(t *testing.T) {
	var i float64
	list := New(CompareFunc(func(lhs interface{}, rhs interface{}) int {
		l := lhs.(float64)
		r := rhs.(float64)
		if l < r {
			return -1
		} else if l == r {
			return 0
		} else {
			return 1
		}
	}))

	if list.maxLevel != DefaultMaxLevel {
		t.Fatal("max level must equal default max value")
	}

	list = NewWithMaxLevel(4, CompareFunc(func(lhs interface{}, rhs interface{}) int {
		l := lhs.(float64)
		r := rhs.(float64)
		if l < r {
			return -1
		} else if l == r {
			return 0
		} else {
			return 1
		}
	}))
	if list.maxLevel != 4 {
		t.Fatal("wrong maxLevel (wanted 4)", list.maxLevel)
	}

	for i = 1; i <= 201; i++ {
		list.Set(i, i*10)
	}

	checkSanity(list, t)

	if list.Length != 201 {
		t.Fatal("wrong list length", list.Length)
	}

	for c := list.Front(); c != nil; c = c.Next() {
		k := c.key.(float64)
		if k*10 != c.value.(float64) {
			t.Fatal("wrong list element value")
		}
	}
}

func TestMaxLevel(t *testing.T) {
	list := NewWithMaxLevel(DefaultMaxLevel+1, CompareFunc(func(lhs interface{}, rhs interface{}) int {
		l := lhs.(int)
		r := rhs.(int)
		if l < r {
			return -1
		} else if l == r {
			return 0
		} else {
			return 1
		}
	}))
	list.Set(0, struct{}{})
}

func TestRemoveUtil(t *testing.T) {
	list := New(CompareFunc(func(lhs interface{}, rhs interface{}) int {
		l := lhs.(int)
		r := rhs.(int)
		if l < r {
			return -1
		} else if l == r {
			return 0
		} else {
			return 1
		}
	}))
	for i := 1; i <= 201; i++ {
		list.Set(i, i*10)
	}
	checkSanity(list, t)
	list.RemoveUntil(10)
	checkSanity(list, t)
	if list.Length != 192 {
		t.Fatal("wrong list length", list.Length)
	}

	start := 10
	for c := list.Front(); c != nil; c = c.Next() {
		k := c.key.(int)
		if k != start {
			t.Fatalf("key order is wrong. expected %d got %d", start, k)
		}
		start += 1
	}

}

func TestChangeProbability(t *testing.T) {
	list := New(CompareFunc(func(lhs interface{}, rhs interface{}) int {
		l := lhs.(int)
		r := rhs.(int)
		if l < r {
			return -1
		} else if l == r {
			return 0
		} else {
			return 1
		}
	}))

	if list.probability != DefaultProbability {
		t.Fatal("new lists should have P value = DefaultProbability")
	}

	list.SetProbability(0.5)
	if list.probability != 0.5 {
		t.Fatal("failed to set new list probability value: expected 0.5, got", list.probability)
	}
}

func TestConcurrency(t *testing.T) {
	list := New(CompareFunc(func(lhs interface{}, rhs interface{}) int {
		l := lhs.(float64)
		r := rhs.(float64)
		if l < r {
			return -1
		} else if l == r {
			return 0
		} else {
			return 1
		}
	}))

	wg := &sync.WaitGroup{}
	wg.Add(2)
	go func() {
		for i := 0; i < 100000; i++ {
			list.Set(float64(i), i)
		}
		wg.Done()
	}()

	go func() {
		for i := 0; i < 100000; i++ {
			list.Get(float64(i))
		}
		wg.Done()
	}()

	wg.Wait()
	if list.Length != 100000 {
		t.Fail()
	}
}

func BenchmarkIncSet(b *testing.B) {
	b.ReportAllocs()
	list := New(CompareFunc(func(lhs interface{}, rhs interface{}) int {
		l := lhs.(float64)
		r := lhs.(float64)
		if l < r {
			return -1
		} else if l == r {
			return 0
		} else {
			return 1
		}
	}))

	for i := 0; i < b.N; i++ {
		list.Set(float64(i), [1]byte{})
	}

	b.SetBytes(int64(b.N))
}

func BenchmarkIncGet(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		res := benchList.Get(float64(i))
		if res == nil {
			b.Fatal("failed to Get an element that should exist")
		}
	}

	b.SetBytes(int64(b.N))
}

func BenchmarkDecSet(b *testing.B) {
	b.ReportAllocs()
	list := New(CompareFunc(func(lhs interface{}, rhs interface{}) int {
		l := lhs.(float64)
		r := lhs.(float64)
		if l < r {
			return -1
		} else if l == r {
			return 0
		} else {
			return 1
		}
	}))

	for i := b.N; i > 0; i-- {
		list.Set(float64(i), [1]byte{})
	}

	b.SetBytes(int64(b.N))
}

func BenchmarkDecGet(b *testing.B) {
	b.ReportAllocs()
	for i := b.N; i > 0; i-- {
		res := benchList.Get(float64(i))
		if res == nil {
			b.Fatal("failed to Get an element that should exist", i)
		}
	}

	b.SetBytes(int64(b.N))
}
