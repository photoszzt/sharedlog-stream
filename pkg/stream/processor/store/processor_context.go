package store

import "log"

type ProcessorContext interface {
	RegisterKeyValueStore(store KeyValueStore)
	RegisterWindowStore(store WindowStore)
	GetKeyValueStore(storeName string) KeyValueStore
	GetWindowStore(storeName string) WindowStore
}

type processorContextImpl struct {
	kvStoreMap     map[string]KeyValueStore
	windowStoreMap map[string]WindowStore
}

func NewProcessorContext() ProcessorContext {
	return &processorContextImpl{
		kvStoreMap: make(map[string]KeyValueStore),
	}
}

func (pctx *processorContextImpl) RegisterKeyValueStore(store KeyValueStore) {
	if _, ok := pctx.kvStoreMap[store.Name()]; ok {
		log.Fatalf("key value store %s already exists", store.Name())
	}
	pctx.kvStoreMap[store.Name()] = store
}

func (pctx *processorContextImpl) RegisterWindowStore(store WindowStore) {
	if _, ok := pctx.windowStoreMap[store.Name()]; ok {
		log.Fatalf("window store %s already exists", store.Name())
	}
	pctx.windowStoreMap[store.Name()] = store
}

func (pctx *processorContextImpl) GetKeyValueStore(storeName string) KeyValueStore {
	return pctx.kvStoreMap[storeName]
}

func (pctx *processorContextImpl) GetWindowStore(storeName string) WindowStore {
	return pctx.windowStoreMap[storeName]
}
