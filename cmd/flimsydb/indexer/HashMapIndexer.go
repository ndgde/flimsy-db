package indexer

import (
	"sync"

	fdb "github.com/ndgde/flimsy-db/cmd/flimsydb"
)

type HashMapIndexer struct {
	mu          sync.RWMutex
	store       map[string][]int
	compareFunc CompareFunc
}

func NewHashMapIndexer(valueType fdb.TabularType) *HashMapIndexer {
	return &HashMapIndexer{
		store:       make(map[string][]int),
		compareFunc: GetCompareFunc(valueType),
	}
}

func bytesToKey(b []byte) string {
	return string(b)
}

func (h *HashMapIndexer) valueExists(key string) ([]int, bool) {
	ptrs, exists := h.store[key]
	return ptrs, exists
}

func (h *HashMapIndexer) Add(val []byte, ptr int) error {
	key := bytesToKey(val)

	h.mu.RLock()
	ptrs, exists := h.valueExists(key)
	if exists {
		for _, p := range ptrs {
			if p == ptr {
				h.mu.RUnlock()
				return fdb.ErrIndexExists
			}
		}
	}
	h.mu.RUnlock()

	h.mu.Lock()
	defer h.mu.Unlock()

	if ptrs, exists = h.valueExists(key); exists {
		for _, p := range ptrs {
			if p == ptr {
				return fdb.ErrIndexExists
			}
		}
		h.store[key] = append(ptrs, ptr)
	} else {
		h.store[key] = []int{ptr}
	}

	return nil
}

func (h *HashMapIndexer) Delete(val []byte, ptr int) error {
	key := bytesToKey(val)

	h.mu.RLock()
	_, exists := h.valueExists(key)
	if !exists {
		h.mu.RUnlock()
		return fdb.ErrIndexNotFound
	}
	h.mu.RUnlock()

	h.mu.Lock()
	defer h.mu.Unlock()

	ptrs, exists := h.valueExists(key)
	if !exists {
		return fdb.ErrIndexNotFound
	}

	for i, p := range ptrs {
		if p == ptr {
			if len(ptrs) == 1 {
				delete(h.store, key)
			} else {
				h.store[key] = append(ptrs[:i], ptrs[i+1:]...)
			}
			return nil
		}
	}

	return fdb.ErrIndexNotFound
}

func (h *HashMapIndexer) Update(oldVal []byte, newVal []byte, ptr int) error {
	if Equal(oldVal, newVal, h.compareFunc) {
		return nil
	}

	oldKey := bytesToKey(oldVal)
	newKey := bytesToKey(newVal)

	h.mu.RLock()
	_, exists := h.valueExists(oldKey)
	if !exists {
		h.mu.RUnlock()
		return fdb.ErrIndexNotFound
	}
	h.mu.RUnlock()

	h.mu.Lock()
	defer h.mu.Unlock()

	var ptrs []int
	ptrs, exists = h.valueExists(oldKey)
	if !exists {
		return fdb.ErrIndexNotFound
	}

	found := false
	for i, p := range ptrs {
		if p == ptr {
			if len(ptrs) == 1 {
				delete(h.store, oldKey)
			} else {
				h.store[oldKey] = append(ptrs[:i], ptrs[i+1:]...)
			}
			found = true
			break
		}
	}

	if !found {
		return fdb.ErrIndexNotFound
	}

	if newPtrs, exists := h.store[newKey]; exists {
		h.store[newKey] = append(newPtrs, ptr)
	} else {
		h.store[newKey] = []int{ptr}
	}

	return nil
}

func (h *HashMapIndexer) Find(val []byte) ([]int, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	key := bytesToKey(val)
	ptrs, exists := h.valueExists(key)
	if !exists {
		return nil, false
	}

	result := make([]int, len(ptrs))
	copy(result, ptrs)
	return result, true
}

func (h *HashMapIndexer) FindInRange(min []byte, max []byte) ([]int, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	var result []int
	for key, ptrs := range h.store {
		keyBytes := []byte(key)
		if LessOrEqual(keyBytes, max, h.compareFunc) && GreaterOrEqual(keyBytes, min, h.compareFunc) {
			result = append(result, ptrs...)
		}
	}

	if len(result) == 0 {
		return nil, false
	}

	return result, true
}
