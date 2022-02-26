package store

import "github.com/google/btree"

type InMemoryBTreeKeyValueStore struct {
	store *btree.BTree
}
