package tests

import (
	"fmt"
	"reflect"
	"sort"
	"sync"
	"testing"

	fdb "github.com/ndgde/flimsy-db/cmd/flimsydb/indexer"
)

type TestKey int

func (k TestKey) Equal(other TestKey) bool          { return k == other }
func (k TestKey) Less(other TestKey) bool           { return k < other }
func (k TestKey) Greater(other TestKey) bool        { return k > other }
func (k TestKey) LessOrEqual(other TestKey) bool    { return k <= other }
func (k TestKey) GreaterOrEqual(other TestKey) bool { return k >= other }

func TestHashMapIndexerOperations(t *testing.T) {
	t.Run("Add", func(t *testing.T) {
		indexer := fdb.NewHashMapIndexer[TestKey]()

		testCases := []struct {
			name    string
			key     TestKey
			ptr     int
			wantErr bool
		}{
			{
				name:    "First addition",
				key:     1,
				ptr:     10,
				wantErr: false,
			},
			{
				name:    "Second addition same key",
				key:     1,
				ptr:     20,
				wantErr: false,
			},
			{
				name:    "Duplicate pointer",
				key:     1,
				ptr:     10,
				wantErr: true,
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				err := indexer.Add(tc.key, tc.ptr)
				if (err != nil) != tc.wantErr {
					t.Errorf("Add() error = %v, wantErr %v", err, tc.wantErr)
				}
			})
		}
	})

	t.Run("Find", func(t *testing.T) {
		indexer := fdb.NewHashMapIndexer[TestKey]()

		// Подготовка данных
		_ = indexer.Add(1, 10)
		_ = indexer.Add(1, 20)

		testCases := []struct {
			name        string
			key         TestKey
			wantPtrs    []int
			wantIsEmpty bool
		}{
			{
				name:        "Existing key",
				key:         1,
				wantPtrs:    []int{10, 20},
				wantIsEmpty: false,
			},
			{
				name:        "Non-existing key",
				key:         2,
				wantPtrs:    []int{},
				wantIsEmpty: true,
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				ptrs, isEmpty := indexer.Find(tc.key)
				if isEmpty != tc.wantIsEmpty {
					t.Errorf("Find() isEmpty = %v, want %v", isEmpty, tc.wantIsEmpty)
				}
				if !tc.wantIsEmpty && !compareIntSlices(ptrs, tc.wantPtrs) {
					t.Errorf("Find() ptrs = %v, want %v", ptrs, tc.wantPtrs)
				}
			})
		}
	})

	t.Run("Update", func(t *testing.T) {
		indexer := fdb.NewHashMapIndexer[TestKey]()

		// Подготовка данных
		_ = indexer.Add(1, 10)

		testCases := []struct {
			name    string
			oldKey  TestKey
			newKey  TestKey
			ptr     int
			wantErr bool
		}{
			{
				name:    "Valid update",
				oldKey:  1,
				newKey:  2,
				ptr:     10,
				wantErr: false,
			},
			{
				name:    "Non-existing key",
				oldKey:  3,
				newKey:  4,
				ptr:     10,
				wantErr: true,
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				err := indexer.Update(tc.oldKey, tc.newKey, tc.ptr)
				if (err != nil) != tc.wantErr {
					t.Errorf("Update() error = %v, wantErr %v", err, tc.wantErr)
				}
			})
		}
	})

	t.Run("FindInRange", func(t *testing.T) {
		indexer := fdb.NewHashMapIndexer[TestKey]()

		// Подготовка данных
		for i := 1; i <= 5; i++ {
			_ = indexer.Add(TestKey(i), i*10)
		}

		testCases := []struct {
			name        string
			min         TestKey
			max         TestKey
			wantPtrs    []int
			wantIsEmpty bool
		}{
			{
				name:        "Valid range",
				min:         2,
				max:         4,
				wantPtrs:    []int{20, 30, 40},
				wantIsEmpty: false,
			},
			{
				name:        "Empty range",
				min:         10,
				max:         20,
				wantPtrs:    []int{},
				wantIsEmpty: true,
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				ptrs, isEmpty := indexer.FindInRange(tc.min, tc.max)
				if isEmpty != tc.wantIsEmpty {
					t.Errorf("FindInRange() isEmpty = %v, want %v", isEmpty, tc.wantIsEmpty)
				}
				if !tc.wantIsEmpty && !compareIntSlices(ptrs, tc.wantPtrs) {
					t.Errorf("FindInRange() ptrs = %v, want %v", ptrs, tc.wantPtrs)
				}
			})
		}
	})

	t.Run("Delete", func(t *testing.T) {
		indexer := fdb.NewHashMapIndexer[TestKey]()

		// Подготовка данных
		_ = indexer.Add(1, 10)
		_ = indexer.Add(1, 20)
		_ = indexer.Add(2, 30)

		testCases := []struct {
			name    string
			key     TestKey
			ptr     int
			wantErr bool
		}{
			{
				name:    "Delete existing entry",
				key:     1,
				ptr:     10,
				wantErr: false,
			},
			{
				name:    "Delete non-existing entry",
				key:     3,
				ptr:     40,
				wantErr: true,
			},
			{
				name:    "Delete with wrong pointer",
				key:     1,
				ptr:     30,
				wantErr: true,
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				err := indexer.Delete(tc.key, tc.ptr)
				if (err != nil) != tc.wantErr {
					t.Errorf("Delete() error = %v, wantErr %v", err, tc.wantErr)
				}
			})
		}
	})

	t.Run("FindInRange edge cases", func(t *testing.T) {
		indexer := fdb.NewHashMapIndexer[TestKey]()

		// Подготовка данных
		testData := []struct {
			key TestKey
			ptr int
		}{
			{1, 10}, {2, 20}, {3, 30}, {4, 40}, {5, 50},
		}

		for _, td := range testData {
			_ = indexer.Add(td.key, td.ptr)
		}

		testCases := []struct {
			name        string
			min         TestKey
			max         TestKey
			wantPtrs    []int
			wantIsEmpty bool
		}{
			{
				name:        "Exact bounds",
				min:         2,
				max:         4,
				wantPtrs:    []int{20, 30, 40},
				wantIsEmpty: false,
			},
			{
				name:        "Min equals Max",
				min:         3,
				max:         3,
				wantPtrs:    []int{30},
				wantIsEmpty: false,
			},
			{
				name:        "Invalid range (min > max)",
				min:         4,
				max:         2,
				wantPtrs:    []int{},
				wantIsEmpty: true,
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				ptrs, isEmpty := indexer.FindInRange(tc.min, tc.max)
				if isEmpty != tc.wantIsEmpty {
					t.Errorf("FindInRange() isEmpty = %v, want %v", isEmpty, tc.wantIsEmpty)
				}
				if !tc.wantIsEmpty {
					// Сортируем результаты перед сравнением
					sort.Ints(ptrs)
					if !compareIntSlices(ptrs, tc.wantPtrs) {
						t.Errorf("FindInRange() ptrs = %v, want %v", ptrs, tc.wantPtrs)
					}
				}
			})
		}
	})
}

// Helper function to verify indexer state
func verifyIndexerState(t *testing.T, indexer *fdb.HashMapIndexer[TestKey], key TestKey, expectedPtrs []int) {
	t.Helper()
	ptrs, isEmpty := indexer.Find(key)
	if isEmpty && len(expectedPtrs) > 0 {
		t.Errorf("Expected non-empty result for key %v", key)
		return
	}
	if !isEmpty && len(expectedPtrs) == 0 {
		t.Errorf("Expected empty result for key %v, got %v", key, ptrs)
		return
	}
	if !compareIntSlices(ptrs, expectedPtrs) {
		t.Errorf("For key %v: got ptrs %v, want %v", key, ptrs, expectedPtrs)
	}
}

func TestHashMapIndexerConcurrency(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping concurrent tests in short mode")
	}

	indexer := fdb.NewHashMapIndexer[TestKey]()
	const numOperations = 1000
	var wg sync.WaitGroup
	errCh := make(chan error, numOperations*4)

	// Execute Add operations first and wait for completion to ensure data consistency
	operations := []struct {
		name string
		fn   func(i int)
	}{
		{
			name: "Add",
			fn: func(i int) {
				if err := indexer.Add(TestKey(i), i); err != nil {
					errCh <- fmt.Errorf("add error: %w", err)
				}
			},
		},
	}

	// Perform all Add operations and wait for completion
	for _, op := range operations {
		t.Run(fmt.Sprintf("Concurrent_%s", op.name), func(t *testing.T) {
			for i := 0; i < numOperations; i++ {
				wg.Add(1)
				go func(i int, operation func(int)) {
					defer wg.Done()
					operation(i)
				}(i, op.fn)
			}
		})
	}
	wg.Wait()

	// Perform Find, Update, and Delete operations
	operations = []struct {
		name string
		fn   func(i int)
	}{
		{
			name: "Find",
			fn: func(i int) {
				if _, isEmpty := indexer.Find(TestKey(i)); isEmpty {
					errCh <- fmt.Errorf("find error: key %d not found", i)
				}
			},
		},
		{
			name: "Update",
			fn: func(i int) {
				if err := indexer.Update(TestKey(i), TestKey(i+numOperations), i); err != nil {
					errCh <- fmt.Errorf("update error: %w", err)
				}
			},
		},
		{
			name: "Delete",
			fn: func(i int) {
				if err := indexer.Delete(TestKey(i+numOperations), i); err != nil {
					errCh <- fmt.Errorf("delete error: %w", err)
				}
			},
		},
	}

	for _, op := range operations {
		t.Run(fmt.Sprintf("Concurrent_%s", op.name), func(t *testing.T) {
			for i := 0; i < numOperations; i++ {
				wg.Add(1)
				go func(i int, operation func(int)) {
					defer wg.Done()
					operation(i)
				}(i, op.fn)
			}
		})
	}

	wg.Wait()
	close(errCh)

	var errors []error
	for err := range errCh {
		errors = append(errors, err)
	}

	if len(errors) > 0 {
		t.Errorf("Concurrent operations failed with %d errors: %v", len(errors), errors)
	}
}

// Helper function to compare slices
func compareIntSlices(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestHashMapIndexerStateVerification(t *testing.T) {
	indexer := fdb.NewHashMapIndexer[TestKey]()

	// Test state verification after operations
	t.Run("State verification after operations", func(t *testing.T) {
		// Add initial data
		_ = indexer.Add(TestKey(1), 10)
		_ = indexer.Add(TestKey(1), 20)
		_ = indexer.Add(TestKey(2), 30)

		// Verify initial state
		verifyIndexerState(t, indexer, TestKey(1), []int{10, 20})
		verifyIndexerState(t, indexer, TestKey(2), []int{30})

		// Perform delete operation
		_ = indexer.Delete(TestKey(1), 10)
		verifyIndexerState(t, indexer, TestKey(1), []int{20})

		// Perform update operation
		_ = indexer.Update(TestKey(2), TestKey(3), 30)
		verifyIndexerState(t, indexer, TestKey(2), []int{})
		verifyIndexerState(t, indexer, TestKey(3), []int{30})

		// Verify non-existing key
		verifyIndexerState(t, indexer, TestKey(4), []int{})
	})
}

func TestHashMapIndexerFindInRange(t *testing.T) {
	indexer := fdb.NewHashMapIndexer[TestKey]()

	// Prepare data
	for i := 1; i <= 5; i++ {
		_ = indexer.Add(TestKey(i), i*10)
	}

	min := TestKey(2)
	max := TestKey(4)

	ptrs, empty := indexer.FindInRange(min, max)
	if empty {
		t.Error("Expected non-empty result")
	}

	// Sort the results before comparing
	sort.Ints(ptrs)

	want := []int{20, 30, 40}
	if !reflect.DeepEqual(ptrs, want) {
		t.Errorf("FindInRange() ptrs = %v, want %v", ptrs, want)
	}
}

func BenchmarkHashMapIndexer(b *testing.B) {
	indexer := fdb.NewHashMapIndexer[TestKey]()

	b.Run("Add", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = indexer.Add(TestKey(i), i)
		}
	})

	b.Run("Find", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, _ = indexer.Find(TestKey(i % 1000))
		}
	})
}

func TestHashMapIndexerMemoryLeaks(t *testing.T) {
	indexer := fdb.NewHashMapIndexer[TestKey]()

	// Add and delete many entries
	for i := 0; i < 10000; i++ {
		_ = indexer.Add(TestKey(i), i)
	}

	for i := 0; i < 10000; i++ {
		_ = indexer.Delete(TestKey(i), i)
	}

	// Verify internal maps are cleaned up
	ptrs, isEmpty := indexer.Find(TestKey(1))
	if !isEmpty || len(ptrs) > 0 {
		t.Error("Indexer should be empty after deleting all entries")
	}
}
