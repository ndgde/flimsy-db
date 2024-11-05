package indexer

import fdb "github.com/ndgde/flimsy-db/cmd/flimsydb"

type Indexer interface {
	Add(val []byte, ptr int) error
	Update(oldVal []byte, newVal []byte, ptr int) error
	Delete(val []byte, ptr int) error
	Find(val []byte) ([]int, bool)
	FindInRange(min []byte, max []byte) ([]int, bool)
}

type IndexerType int

const (
	AbsentIndexerType IndexerType = iota
	HashMapIndexerType
)

func NewIndexer(indexerType IndexerType, valueType fdb.TabularType) Indexer {
	switch indexerType {
	case HashMapIndexerType:
		return NewHashMapIndexer(valueType)
	default:
		return nil
	}
}
