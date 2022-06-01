package store

import "log"

type StoreContext interface {
	RegisterKeyValueStore(store KeyValueStore)
	RegisterWindowStore(store WindowStore)
	GetKeyValueStore(storeName string) KeyValueStore
	GetWindowStore(storeName string) WindowStore
}

type storeContextImpl struct {
	kvStoreMap     map[string]KeyValueStore
	windowStoreMap map[string]WindowStore
}

func NewProcessorContext() StoreContext {
	return &storeContextImpl{
		kvStoreMap: make(map[string]KeyValueStore),
	}
}

func (pctx *storeContextImpl) RegisterKeyValueStore(store KeyValueStore) {
	if _, ok := pctx.kvStoreMap[store.Name()]; ok {
		log.Fatalf("key value store %s already exists", store.Name())
	}
	pctx.kvStoreMap[store.Name()] = store
}

func (pctx *storeContextImpl) RegisterWindowStore(store WindowStore) {
	if _, ok := pctx.windowStoreMap[store.Name()]; ok {
		log.Fatalf("window store %s already exists", store.Name())
	}
	pctx.windowStoreMap[store.Name()] = store
}

func (pctx *storeContextImpl) GetKeyValueStore(storeName string) KeyValueStore {
	return pctx.kvStoreMap[storeName]
}

func (pctx *storeContextImpl) GetWindowStore(storeName string) WindowStore {
	return pctx.windowStoreMap[storeName]
}
