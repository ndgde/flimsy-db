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

	for i, p := range h.store[val] {
		if p == ptr {
			h.store[val] = append(h.store[val][:i], h.store[val][i+1:]...)
			if len(h.store[val]) == 0 {
				delete(h.store, val)
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

	if err := h.Delete(oldVal, ptr); err != nil {
		return err
	}

	if err := h.Add(newVal, ptr); err != nil {
		return err
	}

	return nil
}

func (h *HashMapIndexer[K]) Find(val K) ([]int, bool) { /* returns indexes and empty var ot type bool */
	h.mu.RLock()
	defer h.mu.RUnlock()

	if h.valExists(val) {
		return h.store[val], false
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
