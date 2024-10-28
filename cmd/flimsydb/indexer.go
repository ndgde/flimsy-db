package flimsydb

import (
	"errors"
	"sync"
)

type Indexable[K comparable] interface {
	comparable
	Less(other K) bool
	Greater(other K) bool
	LessOrEqual(other K) bool
	GreaterOrEqual(other K) bool
}

type Indexer[K comparable, I Indexable[K]] interface {
	Add(val I, ptr int) error
	Update(oldVal I, newVal I, ptr int) error
	Delete(val I, ptr int) error
	Find(val I) ([]int, bool)
	FindInRange(min I, max I) ([]int, bool)
}

type HashMapIndexer[K comparable, I Indexable[K]] struct {
	mu    sync.RWMutex
	store map[I][]int
}

func NewHashMapIndexer[K comparable, I Indexable[K]]() *HashMapIndexer[K, I] {
	return &HashMapIndexer[K, I]{
		store: make(map[I][]int),
	}
}

func (h *HashMapIndexer[_, I]) ptrsExists(val I) bool {
	_, exists := h.store[val]
	return exists
}

func (h *HashMapIndexer[_, I]) Add(val I, ptr int) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.ptrsExists(val) {
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

func (h *HashMapIndexer[_, I]) Delete(val I, ptr int) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	if !h.ptrsExists(val) {
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

func (h *HashMapIndexer[_, I]) Update(oldVal I, newVal I, ptr int) error {
	if err := h.Delete(oldVal, ptr); err != nil {
		return err
	}

	if err := h.Add(newVal, ptr); err != nil {
		return err
	}

	return nil
}

func (h *HashMapIndexer[_, I]) Find(val I) ([]int, bool) { /* returns indexes and empty var ot type bool */
	h.mu.RLock()
	defer h.mu.RUnlock()

	if h.ptrsExists(val) {
		return h.store[val], false
	}

	return []int{}, true
}

func (h *HashMapIndexer[K, I]) FindInRange(min K, max K) ([]int, bool) {
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
