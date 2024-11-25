package indexer_test

import (
	"encoding/binary"
	"sync"
	"testing"
	"time"

	cm "github.com/ndgde/flimsy-db/cmd/flimsydb/common"
	"github.com/ndgde/flimsy-db/cmd/flimsydb/indexer"
)

// Helper functions for tests
func int32ToBytes(val int32) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, uint32(val))
	return b
}

func TestBPlusTreeIndexer_Creation(t *testing.T) {
	testCases := []struct {
		name      string
		valueType cm.TabularType
	}{
		{"Int32 type", cm.Int32TType},
		{"String type", cm.StringTType},
		{"Float64 type", cm.Float64TType},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			idx := indexer.NewBPlusTreeIndexer(tc.valueType)
			if idx == nil {
				t.Error("NewBPlusTreeIndexer() returned nil")
			}
		})
	}
}

func TestBPlusTreeIndexer_SingleOperations(t *testing.T) {
	idx := indexer.NewBPlusTreeIndexer(cm.Int32TType)
	val := int32ToBytes(42)
	ptr := 1

	// Test Add
	if err := idx.Add(val, ptr); err != nil {
		t.Fatalf("Add() failed: %v", err)
	}

	// Test Find
	ptrs := idx.Find(val)
	if len(ptrs) != 1 || ptrs[0] != ptr {
		t.Errorf("Find() = %v, want [%d]", ptrs, ptr)
	}

	// Test Delete
	if err := idx.Delete(val, ptr); err != nil {
		t.Errorf("Delete() failed: %v", err)
	}

	// Verify deletion
	ptrs = idx.Find(val)
	if len(ptrs) != 0 {
		t.Errorf("Find() after delete = %v, want []", ptrs)
	}
}

func TestBPlusTreeIndexer_MultipleValues(t *testing.T) {
	idx := indexer.NewBPlusTreeIndexer(cm.Int32TType)

	// Add multiple values
	for i := 0; i < 5; i++ {
		val := int32ToBytes(int32(i))
		if err := idx.Add(val, i); err != nil {
			t.Fatalf("Add(%d) failed: %v", i, err)
		}
	}

	// Verify all values
	for i := 0; i < 5; i++ {
		val := int32ToBytes(int32(i))
		ptrs := idx.Find(val)
		if len(ptrs) != 1 || ptrs[0] != i {
			t.Errorf("Find(%d) = %v, want [%d]", i, ptrs, i)
		}
	}
}

func TestBPlusTreeIndexer_RangeQueries(t *testing.T) {
	idx := indexer.NewBPlusTreeIndexer(cm.Int32TType)

	// Add test data
	for i := 0; i < 10; i++ {
		val := int32ToBytes(int32(i))
		if err := idx.Add(val, i); err != nil {
			t.Fatalf("Failed to add value %d: %v", i, err)
		}
	}

	tests := []struct {
		name     string
		min      int32
		max      int32
		expected int
	}{
		{"Small range", 2, 4, 3},
		{"Single value", 5, 5, 1},
		{"Full range", 0, 9, 10},
		{"Empty range", 20, 30, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := idx.FindInRange(int32ToBytes(tt.min), int32ToBytes(tt.max))
			if len(result) != tt.expected {
				t.Errorf("FindInRange(%d, %d) got %d results, want %d",
					tt.min, tt.max, len(result), tt.expected)
			}
		})
	}
}

func TestBPlusTreeIndexer_EdgeCases(t *testing.T) {
	idx := indexer.NewBPlusTreeIndexer(cm.StringTType)

	testCases := []struct {
		name string
		fn   func(t *testing.T)
	}{
		{"Nil value", func(t *testing.T) {
			err := idx.Add(nil, 1)
			if err == nil {
				t.Error("Expected error for nil value")
			}
		}},
		{"Empty value", func(t *testing.T) {
			err := idx.Add([]byte{}, 1)
			if err != nil {
				t.Errorf("Failed to add empty value: %v", err)
			}
			_ = idx.Delete([]byte{}, 1)
		}},
		{"Negative pointer", func(t *testing.T) {
			err := idx.Add([]byte("test"), -1)
			if err == nil {
				t.Error("Expected error for negative pointer")
			}
		}},
		{"Delete non-existent", func(t *testing.T) {
			err := idx.Delete([]byte("non-existent"), 1)
			if err != cm.ErrIndexNotFound {
				t.Errorf("Expected ErrIndexNotFound, got %v", err)
			}
		}},
		{"Update non-existent", func(t *testing.T) {
			err := idx.Update([]byte("non-existent"), []byte("new"), 1)
			if err != cm.ErrIndexNotFound {
				t.Errorf("Expected ErrIndexNotFound, got %v", err)
			}
		}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tc.fn(t)
		})
	}
}

func TestBPlusTreeIndexer_ConcurrentAccess(t *testing.T) {
	idx := indexer.NewBPlusTreeIndexer(cm.Int32TType)
	const workers = 5
	const opsPerWorker = 20

	var wg sync.WaitGroup
	wg.Add(workers)

	// Add mutex for synchronization access to t.Error
	var mu sync.Mutex

	for i := 0; i < workers; i++ {
		go func(workerID int) {
			defer wg.Done()
			base := workerID * opsPerWorker

			// Add values
			for j := 0; j < opsPerWorker; j++ {
				val := int32ToBytes(int32(base + j))
				if err := idx.Add(val, base+j); err != nil && err != cm.ErrIndexExists {
					mu.Lock()
					t.Errorf("Worker %d: Add failed: %v", workerID, err)
					mu.Unlock()
				}
				// Add a small delay between operations
				time.Sleep(time.Millisecond)
			}

			// Find values
			for j := 0; j < opsPerWorker; j++ {
				val := int32ToBytes(int32(base + j))
				if ptrs := idx.Find(val); len(ptrs) == 0 {
					mu.Lock()
					t.Errorf("Worker %d: Find returned empty result for %d", workerID, base+j)
					mu.Unlock()
				}
				time.Sleep(time.Millisecond)
			}

			// Delete values
			for j := 0; j < opsPerWorker; j++ {
				val := int32ToBytes(int32(base + j))
				if err := idx.Delete(val, base+j); err != nil && err != cm.ErrIndexNotFound {
					mu.Lock()
					t.Errorf("Worker %d: Delete failed: %v", workerID, err)
					mu.Unlock()
				}
				time.Sleep(time.Millisecond)
			}
		}(i)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Test completed successfully
	case <-time.After(5 * time.Second):
		t.Fatal("Test timed out")
	}
}

func TestBPlusTreeIndexer_Update(t *testing.T) {
	idx := indexer.NewBPlusTreeIndexer(cm.Int32TType)

	// Add initial value
	oldVal := int32ToBytes(1)
	newVal := int32ToBytes(2)
	ptr := 42

	if err := idx.Add(oldVal, ptr); err != nil {
		t.Fatalf("Add() failed: %v", err)
	}

	// Test update
	if err := idx.Update(oldVal, newVal, ptr); err != nil {
		t.Errorf("Update() failed: %v", err)
	}

	// Verify old value is gone
	if ptrs := idx.Find(oldVal); len(ptrs) != 0 {
		t.Errorf("Find(oldVal) = %v, want []", ptrs)
	}

	// Verify new value exists
	if ptrs := idx.Find(newVal); len(ptrs) != 1 || ptrs[0] != ptr {
		t.Errorf("Find(newVal) = %v, want [%d]", ptrs, ptr)
	}
}
