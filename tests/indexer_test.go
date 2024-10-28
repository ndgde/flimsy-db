package tests

import (
	"testing"

	fdb "github.com/ndgde/flimsy-db/cmd/flimsydb"
)

type TestKey int

func (k TestKey) Less(other TestKey) bool        { return k < other }
func (k TestKey) Greater(other TestKey) bool     { return k > other }
func (k TestKey) LessOrEqual(other TestKey) bool { return k <= other }
func (k TestKey) GreaterOrEqual(other TestKey) bool {
	return k >= other
}

func TestHashMapIndexer_Add(t *testing.T) {
	indexer := fdb.NewHashMapIndexer[TestKey, TestKey]()

	err := indexer.Add(TestKey(1), 10)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	err = indexer.Add(TestKey(1), 20)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	err = indexer.Add(TestKey(1), 10) // should fail
	if err == nil {
		t.Fatalf("expected error, got none")
	}
}

func TestHashMapIndexer_Find(t *testing.T) {
	indexer := fdb.NewHashMapIndexer[TestKey, TestKey]()

	indexer.Add(TestKey(1), 10)
	indexer.Add(TestKey(1), 20)

	ptrs, isEmpty := indexer.Find(TestKey(1))
	if isEmpty || len(ptrs) != 2 || ptrs[0] != 10 || ptrs[1] != 20 {
		t.Fatalf("expected pointers [10, 20], got %v, isEmpty: %v", ptrs, isEmpty)
	}

	ptrs, isEmpty = indexer.Find(TestKey(2))
	if !isEmpty || len(ptrs) != 0 {
		t.Fatalf("expected isEmpty true, got %v, isEmpty: %v", ptrs, isEmpty)
	}
}

func TestHashMapIndexer_Update(t *testing.T) {
	indexer := fdb.NewHashMapIndexer[TestKey, TestKey]()

	indexer.Add(TestKey(1), 10)

	err := indexer.Update(TestKey(1), TestKey(2), 10)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	ptrs, isEmpty := indexer.Find(TestKey(2))
	if isEmpty || len(ptrs) != 1 || ptrs[0] != 10 {
		t.Fatalf("expected pointer [10], got %v, isEmpty: %v", ptrs, isEmpty)
	}

	ptrs, isEmpty = indexer.Find(TestKey(1))
	if !isEmpty || len(ptrs) != 0 {
		t.Fatalf("expected isEmpty true, got %v, isEmpty: %v", ptrs, isEmpty)
	}
}

func TestHashMapIndexer_Delete(t *testing.T) {
	indexer := fdb.NewHashMapIndexer[TestKey, TestKey]()

	indexer.Add(TestKey(2), 10)

	err := indexer.Delete(TestKey(2), 10)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	ptrs, isEmpty := indexer.Find(TestKey(2))
	if !isEmpty || len(ptrs) != 0 {
		t.Fatalf("expected isEmpty true, got %v, isEmpty: %v", ptrs, isEmpty)
	}
}

func TestHashMapIndexer_Concurrency(t *testing.T) {
	indexer := fdb.NewHashMapIndexer[TestKey, TestKey]()

	// Test concurrent adds
	for i := 0; i < 100; i++ {
		go func(i int) {
			err := indexer.Add(TestKey(i), i)
			if err != nil {
				t.Errorf("expected no error, got %v", err)
			}
		}(i)
	}

	// Test concurrent finds
	for i := 0; i < 100; i++ {
		go func(i int) {
			ptrs, isEmpty := indexer.Find(TestKey(i))
			if !isEmpty && len(ptrs) == 0 {
				t.Errorf("expected non-empty result for key %d", i)
			}
		}(i)
	}
}
