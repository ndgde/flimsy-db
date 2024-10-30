package tests

import (
	"fmt"
	"sync"
	"testing"

	fdb "github.com/ndgde/flimsy-db/cmd/flimsydb"
	idxr "github.com/ndgde/flimsy-db/cmd/flimsydb/indexer"
)

// setupTestTable creates a test table with predefined columns
func setupTestTable(t *testing.T) *fdb.Table {
	t.Helper()
	columns := []*fdb.Column{
		fdb.NewColumn("ID", fdb.Int32ColumnType, fdb.NewInt32Tabular(0), idxr.HashMapIndexerType),
		fdb.NewColumn("Name", fdb.StringColumnType, fdb.NewStringTabular("Unknown"), idxr.AbsentIndexerType),
		fdb.NewColumn("Age", fdb.Int32ColumnType, fdb.NewInt32Tabular(18), idxr.AbsentIndexerType),
		fdb.NewColumn("Salary", fdb.Float64ColumnType, fdb.NewFloat64Tabular(0.0), idxr.AbsentIndexerType),
	}
	return fdb.NewTable(columns)
}

func TestSingleTableOperations(t *testing.T) {
	t.Run("InsertRow", func(t *testing.T) {
		table := setupTestTable(t)

		testCases := []struct {
			name    string
			input   map[string]any
			wantErr bool
		}{
			{
				name: "Valid insert",
				input: map[string]any{
					"ID":     int32(1),
					"Name":   "Alice",
					"Age":    int32(30),
					"Salary": float64(1000.0),
				},
				wantErr: false,
			},
			{
				name: "Invalid type",
				input: map[string]any{
					"ID":   "invalid",
					"Name": "Charlie",
				},
				wantErr: true,
			},
			{
				name: "Invalid column",
				input: map[string]any{
					"InvalidColumn": "value",
				},
				wantErr: true,
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				err := table.InsertRow(tc.input)
				if (err != nil) != tc.wantErr {
					t.Errorf("InsertRow() error = %v, wantErr %v", err, tc.wantErr)
				}
			})
		}
	})

	t.Run("GetRow", func(t *testing.T) {
		table := setupTestTable(t)
		testData := map[string]any{"ID": int32(1), "Name": "Alice", "Age": int32(30)}

		if err := table.InsertRow(testData); err != nil {
			t.Fatalf("Setup failed: %v", err)
		}

		t.Run("Valid index", func(t *testing.T) {
			row, err := table.GetRow(0)
			if err != nil {
				t.Errorf("GetRow() error = %v", err)
				return
			}
			if row["Name"] != "Alice" || row["Age"] != int32(30) {
				t.Errorf("GetRow() = %v, want %v", row, testData)
			}
		})

		t.Run("Invalid index", func(t *testing.T) {
			_, err := table.GetRow(999)
			if err == nil {
				t.Error("GetRow() expected error for invalid index")
			}
		})
	})

	t.Run("UpdateRow", func(t *testing.T) {
		table := setupTestTable(t)

		// Подготовка данных
		if err := table.InsertRow(map[string]any{
			"ID":     int32(1),
			"Name":   "Alice",
			"Salary": float64(1000.0),
		}); err != nil {
			t.Fatalf("Setup failed: %v", err)
		}

		testCases := []struct {
			name    string
			index   int
			updates map[string]any
			wantErr bool
		}{
			{
				name:    "Valid update",
				index:   0,
				updates: map[string]any{"Name": "Bob", "Salary": float64(2000.0)},
				wantErr: false,
			},
			{
				name:    "Invalid index",
				index:   999,
				updates: map[string]any{"Name": "Charlie"},
				wantErr: true,
			},
			{
				name:    "Invalid type",
				index:   0,
				updates: map[string]any{"Salary": "invalid"},
				wantErr: true,
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				err := table.UpdateRow(tc.index, tc.updates)
				if (err != nil) != tc.wantErr {
					t.Errorf("UpdateRow() error = %v, wantErr %v", err, tc.wantErr)
				}
			})
		}
	})

	t.Run("DeleteRow", func(t *testing.T) {
		table := setupTestTable(t)

		if err := table.InsertRow(map[string]any{"ID": int32(1), "Name": "Alice"}); err != nil {
			t.Fatalf("Setup failed: %v", err)
		}

		t.Run("Valid delete", func(t *testing.T) {
			if err := table.DeleteRow(0); err != nil {
				t.Errorf("DeleteRow() error = %v", err)
			}

			// Проверяем, что строка действительно удалена
			if _, err := table.GetRow(0); err == nil {
				t.Error("DeleteRow() row still accessible after deletion")
			}
		})

		t.Run("Invalid index", func(t *testing.T) {
			if err := table.DeleteRow(999); err == nil {
				t.Error("DeleteRow() expected error for invalid index")
			}
		})
	})
}

func TestTableConcurrency(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping concurrent tests in short mode")
	}

	table := setupTestTable(t)
	const numOperations = 1000 // Increased for better race detection
	var wg sync.WaitGroup
	errCh := make(chan error, numOperations*3) // Channel for collecting errors

	t.Run("Concurrent operations", func(t *testing.T) {
		// First perform all inserts
		for i := 0; i < numOperations; i++ {
			err := table.InsertRow(map[string]any{
				"ID":   int32(i),
				"Name": "User",
				"Age":  int32(20),
			})
			if err != nil {
				t.Fatalf("Initial insert failed: %v", err)
			}
		}

		// Now run parallel operations
		for i := 0; i < numOperations; i++ {
			wg.Add(3) // Add one goroutine for reading, updating, and deleting

			// Goroutine for reading
			go func(index int) {
				defer wg.Done()
				_, err := table.GetRow(index)
				if err != nil && err != fdb.ErrRowDeleted && err != fdb.ErrIndexOutOfBounds {
					errCh <- fmt.Errorf("read error at index %d: %v", index, err)
				}
			}(i)

			// Goroutine for updating
			go func(index int) {
				defer wg.Done()
				err := table.UpdateRow(index, map[string]any{"Age": int32(25)})
				if err != nil && err != fdb.ErrRowDeleted && err != fdb.ErrIndexOutOfBounds {
					errCh <- fmt.Errorf("update error at index %d: %v", index, err)
				}
			}(i)

			// Goroutine for deleting
			go func(index int) {
				defer wg.Done()
				err := table.DeleteRow(index)
				if err != nil && err != fdb.ErrRowDeleted && err != fdb.ErrIndexOutOfBounds {
					errCh <- fmt.Errorf("delete error at index %d: %v", index, err)
				}
			}(i)
		}

		// Wait for all operations to complete
		wg.Wait()
		close(errCh)

		// Check for errors
		for err := range errCh {
			t.Errorf("Concurrent operation failed: %v", err)
		}
	})
}

func TestTableCleanup(t *testing.T) {
	table := fdb.NewTable([]*fdb.Column{
		fdb.NewColumn("ID", fdb.Int32ColumnType, fdb.NewInt32Tabular(0), idxr.HashMapIndexerType),
		fdb.NewColumn("Name", fdb.StringColumnType, fdb.NewStringTabular(""), idxr.HashMapIndexerType),
	})

	table.InsertRow(map[string]any{"ID": int32(1), "Name": "one"})
	table.InsertRow(map[string]any{"ID": int32(2), "Name": "two"})
	table.InsertRow(map[string]any{"ID": int32(3), "Name": "three"})

	table.DeleteRow(1) // delete "two"
	table.DeleteRow(3) // delete "four"

	// Cleanup table
	table.Cleanup()

	// Check result
	if len(table.Rows) != 2 {
		t.Errorf("Expected 2 rows after cleanup, got %d", len(table.Rows))
	}

	// Check values and their order
	expectedValues := []int{1, 3}
	for i := range table.Rows {
		row, err := table.GetRow(i)
		if err != nil {
			t.Errorf("Failed to get row %d: %v", i, err)
			continue
		}
		id := row["ID"].(int32)
		if id != int32(expectedValues[i]) {
			t.Errorf("Row %d: got ID %d, want %d", i, id, expectedValues[i])
		}
	}
}
