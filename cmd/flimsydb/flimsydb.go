package flimsydb

import (
	"errors"
)

var (
	ErrKeyExists   = errors.New("key already exists")
	ErrKeyNotFound = errors.New("key not found")
)

type FlimsyDB struct {
	data map[string]string
}

func NewFlimsyDB() *FlimsyDB {
	return &FlimsyDB{
		data: make(map[string]string),
	}
}

func (db *FlimsyDB) keyExists(key string) bool {
	_, exists := db.data[key]
	return exists
}

func (db *FlimsyDB) Create(key, value string) error {
	if db.keyExists(key) {
		return ErrKeyExists
	}
	db.data[key] = value

	return nil
}

func (db *FlimsyDB) Read(key string) (string, error) {
	if !db.keyExists(key) {
		return "", ErrKeyNotFound
	}
	value := db.data[key]

	return value, nil
}

func (db *FlimsyDB) Update(key, value string) error {
	if !db.keyExists(key) {
		return ErrKeyNotFound
	}
	db.data[key] = value

	return nil
}

func (db *FlimsyDB) Delete(key string) error {
	if !db.keyExists(key) {
		return ErrKeyNotFound
	}
	delete(db.data, key)

	return nil
}
