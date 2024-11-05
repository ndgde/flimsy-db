package flimsydb

import (
	"fmt"
	"sync"

	cm "github.com/ndgde/flimsy-db/cmd/flimsydb/common"
)

type FlimsyDB struct {
	mu     sync.RWMutex
	tables map[string]*Table
}

func NewFlimsyDB() *FlimsyDB {
	return &FlimsyDB{
		tables: make(map[string]*Table),
	}
}

func (db *FlimsyDB) CreateTable(name string, columns []*Column) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.tableExists(name) {
		return cm.ErrTableExists
	}

	db.tables[name] = NewTable(columns)
	return nil
}

func (db *FlimsyDB) GetTable(name string) (*Table, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	table, exists := db.tables[name]
	if !exists {
		return nil, cm.ErrTableNotFound
	}
	return table, nil
}

func (db *FlimsyDB) DeleteTable(name string) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if !db.tableExists(name) {
		return cm.ErrTableNotFound
	}
	delete(db.tables, name)
	return nil
}

func (db *FlimsyDB) tableExists(name string) bool {
	_, exists := db.tables[name]
	return exists
}

func (db *FlimsyDB) TableExists(name string) bool {
	db.mu.RLock()
	defer db.mu.RUnlock()

	_, exists := db.tables[name]
	return exists
}

func (db *FlimsyDB) ListTables() []string {
	db.mu.RLock()
	defer db.mu.RUnlock()

	tables := make([]string, 0, len(db.tables))
	for name := range db.tables {
		tables = append(tables, name)
	}
	return tables
}

func (db *FlimsyDB) PrintTables() {
	for name := range db.tables {
		fmt.Printf("•%v\n", name)
	}
}
