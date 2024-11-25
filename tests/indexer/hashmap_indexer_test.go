package indexer_test

import (
	"fmt"
	"reflect"
	"sort"
	"sync"
	"testing"

	cm "github.com/ndgde/flimsy-db/cmd/flimsydb/common"
	"github.com/ndgde/flimsy-db/cmd/flimsydb/indexer"
)

func TestHashMapIndexerCreation(t *testing.T) {
	testCases := []struct {
		name       string
		indexType  indexer.IndexerType
		valueType  cm.TabularType
		wantNil    bool
		indexerStr string
	}{
		{
			name:       "Create HashMap indexer",
			indexType:  indexer.HashMapIndexerType,
			valueType:  cm.Int32TType,
			wantNil:    false,
			indexerStr: "*indexer.HashMapIndexer",
		},
		{
			name:      "Create with invalid type",
			indexType: indexer.AbsentIndexerType,
			valueType: cm.Int32TType,
			wantNil:   true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			idx := indexer.NewIndexer(tc.indexType, tc.valueType)
			if (idx == nil) != tc.wantNil {
				t.Errorf("NewIndexer() returned nil: %v, want nil: %v", idx == nil, tc.wantNil)
			}
			if !tc.wantNil && tc.indexerStr != "" {
				actualType := fmt.Sprintf("%T", idx)
				if actualType != tc.indexerStr {
					t.Errorf("Expected indexer type %s, got %s", tc.indexerStr, actualType)
				}
			}
		})
	}
}

func TestHashMapIndexerOperations(t *testing.T) {
	idx := indexer.NewHashMapIndexer(cm.StringTType)

	// Test data
	val1 := []byte("test1")
	val2 := []byte("test2")
	val3 := []byte("test3")

	// Test Add
	t.Run("Add operations", func(t *testing.T) {
		if err := idx.Add(val1, 1); err != nil {
			t.Errorf("Failed to add first value: %v", err)
		}

		if err := idx.Add(val1, 1); err != cm.ErrIndexExists {
			t.Errorf("Expected ErrIndexExists when adding duplicate, got %v", err)
		}

		if err := idx.Add(val1, 2); err != nil {
			t.Errorf("Failed to add same value with different pointer: %v", err)
		}

		if err := idx.Add(val2, 3); err != nil {
			t.Errorf("Failed to add different value: %v", err)
		}
	})

	// Test Find
	t.Run("Find operations", func(t *testing.T) {
		ptrs := idx.Find(val1)
		if len(ptrs) != 2 {
			t.Errorf("Expected 2 pointers, got %d", len(ptrs))
		}

		ptrs = idx.Find(val3)
		if len(ptrs) != 0 {
			t.Errorf("Expected 0 pointers for non-existing value, got %d", len(ptrs))
		}
	})

	// Test Delete
	t.Run("Delete operations", func(t *testing.T) {
		if err := idx.Delete(val1, 1); err != nil {
			t.Errorf("Failed to delete existing value: %v", err)
		}

		if err := idx.Delete(val1, 1); err != cm.ErrIndexNotFound {
			t.Errorf("Expected ErrIndexNotFound when deleting non-existing value, got %v", err)
		}

		ptrs := idx.Find(val1)
		if len(ptrs) != 1 {
			t.Errorf("Expected 1 pointer after deletion, got %d", len(ptrs))
		}
	})
}

func TestHashMapFindInRange(t *testing.T) {
	idx := indexer.NewHashMapIndexer(cm.StringTType)

	// Add test data
	testData := []struct {
		value []byte
		ptr   int
	}{
		{[]byte("a"), 1},
		{[]byte("b"), 2},
		{[]byte("c"), 3},
		{[]byte("d"), 4},
		{[]byte("e"), 5},
	}

	for _, td := range testData {
		if err := idx.Add(td.value, td.ptr); err != nil {
			t.Fatalf("Failed to add test data: %v", err)
		}
	}

	testCases := []struct {
		name      string
		min       []byte
		max       []byte
		wantPtrs  []int
		wantFound bool
	}{
		{
			name:      "Find in valid range",
			min:       []byte("b"),
			max:       []byte("d"),
			wantPtrs:  []int{2, 3, 4},
			wantFound: true,
		},
		{
			name:      "Find in empty range",
			min:       []byte("x"),
			max:       []byte("z"),
			wantPtrs:  nil,
			wantFound: false,
		},
		{
			name:      "Find in range with single value",
			min:       []byte("c"),
			max:       []byte("c"),
			wantPtrs:  []int{3},
			wantFound: true,
		},
		{
			name:      "Find in full range",
			min:       []byte("a"),
			max:       []byte("e"),
			wantPtrs:  []int{1, 2, 3, 4, 5},
			wantFound: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ptrs := idx.FindInRange(tc.min, tc.max)

			if tc.wantPtrs != nil {
				if len(ptrs) != len(tc.wantPtrs) {
					t.Errorf("FindInRange() returned %d pointers, want %d", len(ptrs), len(tc.wantPtrs))
					return
				}

				sort.Ints(ptrs)
				sort.Ints(tc.wantPtrs)

				if !reflect.DeepEqual(ptrs, tc.wantPtrs) {
					t.Errorf("FindInRange() returned pointers %v, want %v", ptrs, tc.wantPtrs)
				}
			}
		})
	}
}

func TestHashMapConcurrentAccess(t *testing.T) {
	idx := indexer.NewHashMapIndexer(cm.StringTType)
	const goroutines = 10
	const operationsPerGoroutine = 100

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for i := 0; i < goroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < operationsPerGoroutine; j++ {
				val := []byte(fmt.Sprintf("test%d-%d", id, j))
				newVal := []byte(fmt.Sprintf("new%d-%d", id, j))

				if err := idx.Add(val, j); err != nil && err != cm.ErrIndexExists {
					t.Errorf("Concurrent Add failed: %v", err)
				}

				if err := idx.Update(val, newVal, j); err != nil && err != cm.ErrIndexNotFound {
					t.Errorf("Concurrent Update failed: %v", err)
				}

				if err := idx.Delete(newVal, j); err != nil && err != cm.ErrIndexNotFound {
					t.Errorf("Concurrent Delete failed: %v", err)
				}
			}
		}(i)
	}

	wg.Wait()
}
