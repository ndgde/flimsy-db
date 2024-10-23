package flimsydb

import (
	"errors"
	"sync"
)

var (
	ErrKeyExists   = errors.New("key already exists")
	ErrKeyNotFound = errors.New("key not found")
)

type FlimsyDB struct {
	data map[string]string
	mu   sync.RWMutex
}

func NewFlimsyDB() *FlimsyDB {
	return &FlimsyDB{
		data: make(map[string]string),
	}
}

func (db *FlimsyDB) keyExists(key string) bool {
	db.mu.RLock()
	defer db.mu.RUnlock()
	_, exists := db.data[key]
	return exists
}

func (db *FlimsyDB) Create(key, value string) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.keyExists(key) {
		return ErrKeyExists
	}
	db.data[key] = value

	return nil
}

func (db *FlimsyDB) Read(key string) (string, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if !db.keyExists(key) {
		return "", ErrKeyNotFound
	}
	value := db.data[key]

	return value, nil
}

func (db *FlimsyDB) Update(key, value string) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if !db.keyExists(key) {
		return ErrKeyNotFound
	}
	db.data[key] = value

	return nil
}

func (db *FlimsyDB) Delete(key string) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if !db.keyExists(key) {
		return ErrKeyNotFound
	}
	delete(db.data, key)

	return nil
}
