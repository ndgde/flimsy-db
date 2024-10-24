package flimsydb

import (
	"errors"
	"fmt"
	"sync"
)

var (
	ErrTableExists   = errors.New("key already exists")
	ErrTableNotFound = errors.New("key not found")
)

type FlimsyDB struct {
	tables map[string]*Table
	mu     sync.RWMutex
}

func NewFlimsyDB() *FlimsyDB {
	return &FlimsyDB{
		tables: make(map[string]*Table),
	}
}

func (db *FlimsyDB) TableExists(name string) bool {
	_, exists := db.tables[name]
	return exists
}

func (db *FlimsyDB) CreateTable(name string, columns []Column) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.TableExists(name) {
		return ErrTableExists
	}
	db.tables[name] = NewTable(columns)

	return nil
}

func (db *FlimsyDB) GetTable(name string) (*Table, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if !db.TableExists(name) {
		return nil, ErrTableNotFound
	}

	return db.tables[name], nil
}

func (db *FlimsyDB) Delete(name string) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if !db.TableExists(name) {
		return ErrTableNotFound
	}
	delete(db.tables, name)

	return nil
}

func (db *FlimsyDB) AllTables() map[string]*Table {
	db.mu.RLock()
	defer db.mu.RUnlock()
	result := make(map[string]*Table)
	for k, v := range db.tables {
		result[k] = v
	}

	return result
}

func (db *FlimsyDB) PrintTables() {
	db.mu.RLock()
	defer db.mu.RUnlock()

	for name := range db.tables {
		fmt.Printf("•%v\n", name)
	}
}
