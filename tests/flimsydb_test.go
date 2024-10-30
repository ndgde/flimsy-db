package tests

import (
	"io"
	"os"
	"strings"
	"testing"

	fdb "github.com/ndgde/flimsy-db/cmd/flimsydb"
	"github.com/ndgde/flimsy-db/cmd/flimsydb/indexer"
)

// setupTestDB creates a test database with basic table configuration
func setupTestDB(t *testing.T) (*fdb.FlimsyDB, string, []*fdb.Column) {
	t.Helper()
	db := fdb.NewFlimsyDB()
	tableName := "TestTable"
	columns := []*fdb.Column{
		fdb.NewColumn("ID", fdb.Int32ColumnType, fdb.NewInt32Tabular(0), indexer.HashMapIndexerType),
		fdb.NewColumn("Name", fdb.StringColumnType, fdb.NewStringTabular(""), indexer.AbsentIndexerType),
	}
	return db, tableName, columns
}

func TestDatabaseTableOperations(t *testing.T) {
	t.Run("CreateTable", func(t *testing.T) {
		db, tableName, columns := setupTestDB(t)

		err := db.CreateTable(tableName, columns)
		if err != nil {
			t.Fatalf("CreateTable failed: %v", err)
		}

		if !db.TableExists(tableName) {
			t.Error("Created table does not exist")
		}

		// Test duplicate creation
		err = db.CreateTable(tableName, columns)
		if err != fdb.ErrTableExists {
			t.Errorf("Expected ErrTableExists, got %v", err)
		}
	})

	t.Run("GetTable", func(t *testing.T) {
		db, tableName, columns := setupTestDB(t)

		// Create table for testing
		err := db.CreateTable(tableName, columns)
		if err != nil {
			t.Fatalf("Setup failed: %v", err)
		}

		// Test successful retrieval
		table, err := db.GetTable(tableName)
		if err != nil {
			t.Errorf("GetTable failed: %v", err)
		}
		if table == nil {
			t.Error("GetTable returned nil table")
		}

		// Test non-existent table retrieval
		_, err = db.GetTable("NonExistentTable")
		if err != fdb.ErrTableNotFound {
			t.Errorf("Expected ErrTableNotFound, got %v", err)
		}
	})

	t.Run("DeleteTable", func(t *testing.T) {
		db, tableName, columns := setupTestDB(t)

		err := db.CreateTable(tableName, columns)
		if err != nil {
			t.Fatalf("Setup failed: %v", err)
		}

		err = db.DeleteTable(tableName)
		if err != nil {
			t.Errorf("DeleteTable failed: %v", err)
		}
		if db.TableExists(tableName) {
			t.Error("Table still exists after deletion")
		}

		// Test deleting non-existent table
		err = db.DeleteTable("NonExistentTable")
		if err != fdb.ErrTableNotFound {
			t.Errorf("Expected ErrTableNotFound, got %v", err)
		}
	})
}

func TestDatabaseManipulation(t *testing.T) {
	t.Run("InsertAndRetrieveRow", func(t *testing.T) {
		db, tableName, columns := setupTestDB(t)

		err := db.CreateTable(tableName, columns)
		if err != nil {
			t.Fatalf("Setup failed: %v", err)
		}

		table, err := db.GetTable(tableName)
		if err != nil {
			t.Fatalf("Failed to get table: %v", err)
		}

		// Test data insertion
		testData := map[string]any{
			"ID":   int32(1),
			"Name": "Test User",
		}

		err = table.InsertRow(testData)
		if err != nil {
			t.Errorf("InsertRow failed: %v", err)
		}

		// Test data retrieval
		retrieved, err := table.GetRow(0)
		if err != nil {
			t.Errorf("GetRow failed: %v", err)
		}

		if retrieved["ID"] != int32(1) || retrieved["Name"] != "Test User" {
			t.Error("Retrieved data doesn't match inserted data")
		}
	})
}

func TestDatabaseOutput(t *testing.T) {
	db, tableName, columns := setupTestDB(t)

	err := db.CreateTable(tableName, columns)
	if err != nil {
		t.Fatalf("Setup failed: %v", err)
	}

	// Capture stdout
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	// Cleanup after test
	defer func() {
		os.Stdout = oldStdout
	}()

	// Execute print
	go func() {
		db.PrintTables()
		w.Close()
	}()

	// Read output
	var out strings.Builder
	_, err = io.Copy(&out, r)
	if err != nil {
		t.Fatalf("Failed to capture output: %v", err)
	}

	output := out.String()
	if !strings.Contains(output, tableName) {
		t.Errorf("Expected output to contain '%s', got: %s", tableName, output)
	}
}
