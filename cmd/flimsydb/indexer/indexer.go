package indexer

type Orderable[T any] interface {
	Equal(other T) bool
	Less(other T) bool
	LessOrEqual(other T) bool
	Greater(other T) bool
	GreaterOrEqual(other T) bool
}

type Indexable[T any] interface {
	Orderable[T]
	comparable
}

type Indexer[T Indexable[T]] interface {
	Add(val T, ptr int) error
	Update(oldVal T, newVal T, ptr int) error
	Delete(val T, ptr int) error
	Find(val T) ([]int, bool)
	FindInRange(min T, max T) ([]int, bool)
}

type IndexerType int

const (
	HashMapIndexerType IndexerType = iota
)

func NewIndexer[T Indexable[T]](indexerType IndexerType) Indexer[T] {
	switch indexerType {
	case HashMapIndexerType:
		return NewHashMapIndexer[T]()

	default:
		return nil
	}
}
