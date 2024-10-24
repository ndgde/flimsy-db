package flimsydb

import (
	"errors"
	"sync"
)

var (
	ErrKeyExists   = errors.New("key already exists")
	ErrKeyNotFound = errors.New("key not found")
)

type Database interface {
	Create(key string, value []byte) error
	Read(key string) ([]byte, error)
	Update(key string, value []byte) error
	Delete(key string) error
	All() map[string][]byte
}

type FlimsyDB struct {
	data map[string][]byte
	mu   sync.RWMutex
}

func NewFlimsyDB() *FlimsyDB {
	return &FlimsyDB{
		data: make(map[string][]byte),
	}
}

func (db *FlimsyDB) keyExists(key string) bool {
	_, exists := db.data[key]
	return exists
}

func (db *FlimsyDB) Create(key string, value []byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.keyExists(key) {
		return ErrKeyExists
	}
	db.data[key] = value

	return nil
}

func (db *FlimsyDB) Read(key string) ([]byte, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if !db.keyExists(key) {
		return nil, ErrKeyNotFound
	}
	value := db.data[key]

	return value, nil
}

func (db *FlimsyDB) Update(key string, value []byte) error {
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

func (db *FlimsyDB) All() map[string][]byte {
	db.mu.RLock()
	defer db.mu.RUnlock()
	result := make(map[string][]byte)
	for k, v := range db.data {
		result[k] = v

	}
	return result
}
