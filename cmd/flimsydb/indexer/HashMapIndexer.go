package indexer

import (
	"errors"
	"sync"
)

type HashMapIndexer[K Indexable[K]] struct {
	mu    sync.RWMutex
	store map[K][]int
}

func NewHashMapIndexer[K Indexable[K]]() *HashMapIndexer[K] {
	return &HashMapIndexer[K]{
		store: make(map[K][]int),
	}
}

func (h *HashMapIndexer[K]) valExists(val K) bool {
	_, exists := h.store[val]
	return exists
}

func (h *HashMapIndexer[K]) Add(val K, ptr int) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.valExists(val) {
		for _, p := range h.store[val] {
			if p == ptr {
				return errors.New("index already exists")
			}
		}

		h.store[val] = append(h.store[val], ptr)

	} else {
		h.store[val] = []int{ptr}
	}

	return nil
}

func (h *HashMapIndexer[K]) Delete(val K, ptr int) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	if !h.valExists(val) {
		return errors.New("pointer not exists")
	}

	ptrs := h.store[val]
	for i, p := range ptrs {
		if p == ptr {
			if len(ptrs) == 1 {
				delete(h.store, val)
			} else {
				h.store[val] = append(ptrs[:i], ptrs[i+1:]...)
			}
			return nil
		}
	}

	return errors.New("pointer not exists")
}

func (h *HashMapIndexer[K]) Update(oldVal K, newVal K, ptr int) error {
	if newVal.Equal(oldVal) {
		return nil
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	if !h.valExists(oldVal) {
		return errors.New("old value not found")
	}

	oldPtrs := h.store[oldVal]
	found := false
	for i, p := range oldPtrs {
		if p == ptr {
			if len(oldPtrs) == 1 {
				delete(h.store, oldVal)
			} else {
				h.store[oldVal] = append(oldPtrs[:i], oldPtrs[i+1:]...)
			}
			found = true
			break
		}
	}

	if !found {
		return errors.New("pointer not exists")
	}

	if h.valExists(newVal) {
		h.store[newVal] = append(h.store[newVal], ptr)
	} else {
		h.store[newVal] = []int{ptr}
	}

	return nil
}

func (h *HashMapIndexer[K]) Find(val K) ([]int, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if h.valExists(val) {
		result := make([]int, len(h.store[val]))
		copy(result, h.store[val])
		return result, false
	}

	return []int{}, true
}

func (h *HashMapIndexer[K]) FindInRange(min K, max K) ([]int, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	var result []int
	for val, ptrs := range h.store {
		if val.LessOrEqual(max) && val.GreaterOrEqual(min) {
			result = append(result, ptrs...)
		}
	}

	if len(result) == 0 {
		return result, true
	}

	return result, false
}
