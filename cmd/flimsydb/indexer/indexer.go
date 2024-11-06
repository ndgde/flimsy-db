package indexer

import (
	cm "github.com/ndgde/flimsy-db/cmd/flimsydb/common"
)

type Indexer interface {
	Add(val []byte, ptr int) error
	Update(oldVal []byte, newVal []byte, ptr int) error
	Delete(val []byte, ptr int) error
	Find(val []byte) []int
	FindInRange(min []byte, max []byte) []int
}

type IndexerType int

const (
	AbsentIndexerType IndexerType = iota
	HashMapIndexerType
)

func NewIndexer(indexerType IndexerType, valueType cm.TabularType) Indexer {
	switch indexerType {
	case HashMapIndexerType:
		return NewHashMapIndexer(valueType)
	default:
		return nil
	}
}
