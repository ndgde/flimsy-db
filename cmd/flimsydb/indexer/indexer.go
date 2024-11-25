package indexer

import (
	cm "github.com/ndgde/flimsy-db/cmd/flimsydb/common"
)

type Indexer interface {
	Add(val cm.Blob, ptr int) error
	Update(oldVal cm.Blob, newVal cm.Blob, ptr int) error
	Delete(val cm.Blob, ptr int) error
	Find(val cm.Blob) []int
	FindInRange(min cm.Blob, max cm.Blob) []int
}

type IndexerType int

const (
	AbsentIndexerType IndexerType = iota
	HashMapIndexerType
	BTreeIndexerType
)

func calculateDegree(pageSize int, keySize int, pointerSize int, overhead int) int {
	availableSpace := pageSize - overhead
	recordSize := keySize + pointerSize
	maxKeys := availableSpace / recordSize
	return maxKeys + 1
}

func NewIndexer(indexerType IndexerType, valueType cm.TabularType) Indexer {
	switch indexerType {
	case HashMapIndexerType:
		return NewHashMapIndexer(valueType)
	case BTreeIndexerType:
		return NewBTreeIndexer(valueType, calculateDegree(4096, 8, 8, 64))
	default:
		return nil
	}
}
