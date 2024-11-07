package tests

import (
	"testing"

	flimsydb "github.com/ndgde/flimsy-db/cmd/flimsydb"
	cm "github.com/ndgde/flimsy-db/cmd/flimsydb/common"
	"github.com/ndgde/flimsy-db/cmd/flimsydb/indexer"
)

func TestDatabaseCreation(t *testing.T) {
	db := flimsydb.NewFlimsyDB()
	if db == nil {
		t.Fatal("Failed to create database instance")
	}
}

func TestTableOperations(t *testing.T) {
	db := flimsydb.NewFlimsyDB()

	col1, err := flimsydb.NewColumn("id", cm.Int32TType, int32(0), indexer.AbsentIndexerType, 0)
	if err != nil {
		t.Fatalf("Failed to create column 'id': %v", err)
	}

	col2, err := flimsydb.NewColumn("name", cm.StringTType, "", indexer.AbsentIndexerType, 0)
	if err != nil {
		t.Fatalf("Failed to create column 'name': %v", err)
	}

	col3, err := flimsydb.NewColumn("score", cm.Float64TType, float64(0), indexer.AbsentIndexerType, 0)
	if err != nil {
		t.Fatalf("Failed to create column 'score': %v", err)
	}

	columns := []*flimsydb.Column{col1, col2, col3}

	tableName := "test_table"
	err = db.CreateTable(tableName, columns)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	if err = db.CreateTable(tableName, columns); err == nil {
		t.Error("Expected error when creating duplicate table, got nil")
	}

	if !db.TableExists(tableName) {
		t.Error("Table should exist but TableExists returned false")
	}

	if db.TableExists("non_existent") {
		t.Error("Non-existent table should not exist but TableExists returned true")
	}

	table, err := db.GetTable(tableName)
	if err != nil {
		t.Errorf("Failed to get existing table: %v", err)
	}
	if table == nil {
		t.Error("GetTable returned nil for existing table")
	}

	err = db.DeleteTable(tableName)
	if err != nil {
		t.Errorf("Failed to delete table: %v", err)
	}

	if db.TableExists(tableName) {
		t.Error("Table still exists after deletion")
	}
}

func TestDataOperations(t *testing.T) {
	db := flimsydb.NewFlimsyDB()
	tableName := "test_data"

	col1, err := flimsydb.NewColumn("id", cm.Int32TType, int32(0), indexer.AbsentIndexerType, 0)
	if err != nil {
		t.Fatalf("Failed to create column 'id': %v", err)
	}

	col2, err := flimsydb.NewColumn("name", cm.StringTType, "", indexer.AbsentIndexerType, 0)
	if err != nil {
		t.Fatalf("Failed to create column 'name': %v", err)
	}

	col3, err := flimsydb.NewColumn("score", cm.Float64TType, float64(0), indexer.AbsentIndexerType, 0)
	if err != nil {
		t.Fatalf("Failed to create column 'score': %v", err)
	}

	columns := []*flimsydb.Column{col1, col2, col3}

	err = db.CreateTable(tableName, columns)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	testData := []map[string]any{
		{
			"id":    int32(1),
			"name":  "Alice",
			"score": float64(85.5),
		},
		{
			"id":    int32(2),
			"name":  "Bob",
			"score": float64(92.0),
		},
	}

	table, err := db.GetTable(tableName)
	if err != nil {
		t.Fatalf("Failed to get table: %v", err)
	}

	for _, data := range testData {
		err := table.InsertRow(data)
		if err != nil {
			t.Errorf("Failed to insert data: %v", err)
		}
	}

	_, err = table.GetRow(0)
	if err != nil {
		t.Fatalf("Failed to get row: %v", err)
	}

	updateData := map[string]any{
		"score": float64(95.0),
	}
	err = table.UpdateRow(0, updateData)
	if err != nil {
		t.Errorf("Failed to update data: %v", err)
	}

	updatedRow, err := table.GetRow(0)
	if err != nil {
		t.Fatalf("Failed to get updated row: %v", err)
	}

	score, err := flimsydb.Deserialize(cm.Float64TType, updatedRow[2])
	if err != nil {
		t.Fatalf("Failed to deserialize score: %v", err)
	}

	if score.(float64) != 95.0 {
		t.Errorf("Expected updated score to be 95.0, got %v", score)
	}

	err = table.DeleteRow(1)
	if err != nil {
		t.Errorf("Failed to delete row: %v", err)
	}
}

func TestErrorCases(t *testing.T) {
	db := flimsydb.NewFlimsyDB()
	tableName := "error_test"

	t.Run("Operations on non-existent table", func(t *testing.T) {
		_, err := db.GetTable(tableName)
		if err == nil {
			t.Error("Expected error when getting non-existent table")
		}

		err = db.DeleteTable(tableName)
		if err == nil {
			t.Error("Expected error when deleting non-existent table")
		}
	})

	col1, err := flimsydb.NewColumn("id", cm.Int32TType, int32(0), indexer.AbsentIndexerType, 0)
	if err != nil {
		t.Fatalf("Failed to create column 'id': %v", err)
	}

	col2, err := flimsydb.NewColumn("name", cm.StringTType, "", indexer.AbsentIndexerType, 0)
	if err != nil {
		t.Fatalf("Failed to create column 'name': %v", err)
	}

	columns := []*flimsydb.Column{col1, col2}

	err = db.CreateTable(tableName, columns)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	table, err := db.GetTable(tableName)
	if err != nil {
		t.Fatalf("Failed to get table: %v", err)
	}

	t.Run("Invalid data types", func(t *testing.T) {
		invalidData := map[string]any{
			"id":   "not an int32",
			"name": "valid string",
		}

		err := table.InsertRow(invalidData)
		if err == nil {
			t.Error("Expected error when inserting invalid data type")
		}
	})

	t.Run("Invalid column names", func(t *testing.T) {
		invalidData := map[string]any{
			"id":          int32(1),
			"nonexistent": "value",
		}

		err := table.InsertRow(invalidData)
		if err == nil {
			t.Error("Expected error when inserting data with invalid column name")
		}
	})
}

func TestListTables(t *testing.T) {
	db := flimsydb.NewFlimsyDB()

	tables := []string{"table1", "table2", "table3"}

	col, err := flimsydb.NewColumn("id", cm.Int32TType, int32(0), indexer.AbsentIndexerType, 0)
	if err != nil {
		t.Fatalf("Failed to create column: %v", err)
	}

	columns := []*flimsydb.Column{col}

	for _, name := range tables {
		err := db.CreateTable(name, columns)
		if err != nil {
			t.Fatalf("Failed to create table %s: %v", name, err)
		}
	}

	listed := db.ListTables()
	if len(listed) != len(tables) {
		t.Errorf("Expected %d tables, got %d", len(tables), len(listed))
	}

	for _, name := range tables {
		found := false
		for _, listedName := range listed {
			if name == listedName {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Table %s not found in listed tables", name)
		}
	}
}
