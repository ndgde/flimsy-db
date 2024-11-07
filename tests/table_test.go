package tests

import (
	"testing"

	flimsydb "github.com/ndgde/flimsy-db/cmd/flimsydb"
	cm "github.com/ndgde/flimsy-db/cmd/flimsydb/common"
	"github.com/ndgde/flimsy-db/cmd/flimsydb/indexer"
)

func TestTableCreation(t *testing.T) {
	col, err := flimsydb.NewColumn("id", cm.Int32TType, int32(0), indexer.AbsentIndexerType, 0)
	if err != nil {
		t.Fatalf("Failed to create column: %v", err)
	}

	col2, err := flimsydb.NewColumn("name", cm.StringTType, "", indexer.AbsentIndexerType, 0)
	if err != nil {
		t.Fatalf("Failed to create column 'name': %v", err)
	}

	col3, err := flimsydb.NewColumn("score", cm.Float64TType, float64(0), indexer.AbsentIndexerType, 0)
	if err != nil {
		t.Fatalf("Failed to create column 'score': %v", err)
	}

	columns := []*flimsydb.Column{col, col2, col3}
	flimsydb.NewTable(columns)
}

func TestInsertRow(t *testing.T) {
	col1, err := flimsydb.NewColumn("id", cm.Int32TType, int32(0), indexer.AbsentIndexerType, 0)
	if err != nil {
		t.Fatalf("Failed to create column 'id': %v", err)
	}

	col2, err := flimsydb.NewColumn("name", cm.StringTType, "", indexer.AbsentIndexerType, 0)
	if err != nil {
		t.Fatalf("Failed to create column 'name': %v", err)
	}

	columns := []*flimsydb.Column{col1, col2}
	table := flimsydb.NewTable(columns)

	testCases := []struct {
		name    string
		values  map[string]any
		wantErr bool
	}{
		{
			name: "Valid insertion",
			values: map[string]any{
				"id":   int32(1),
				"name": "Test",
			},
			wantErr: false,
		},
		{
			name: "Invalid data type",
			values: map[string]any{
				"id":   "wrong type",
				"name": "Test",
			},
			wantErr: true,
		},
		{
			name: "Non-existent column",
			values: map[string]any{
				"id":      int32(1),
				"unknown": "value",
			},
			wantErr: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := table.InsertRow(tc.values)
			if (err != nil) != tc.wantErr {
				t.Errorf("InsertRow() error = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}

func TestUpdateAndGetRow(t *testing.T) {
	col1, err := flimsydb.NewColumn("id", cm.Int32TType, int32(0), indexer.AbsentIndexerType, 0)
	if err != nil {
		t.Fatalf("Failed to create column 'id': %v", err)
	}

	col2, err := flimsydb.NewColumn("name", cm.StringTType, "", indexer.AbsentIndexerType, 0)
	if err != nil {
		t.Fatalf("Failed to create column 'name': %v", err)
	}

	columns := []*flimsydb.Column{col1, col2}
	table := flimsydb.NewTable(columns)

	initialValues := map[string]any{
		"id":   int32(1),
		"name": "Initial",
	}
	if err := table.InsertRow(initialValues); err != nil {
		t.Fatalf("Error inserting initial data: %v", err)
	}

	row, err := table.GetRow(0)
	if err != nil {
		t.Fatalf("Error getting row: %v", err)
	}

	id, err := flimsydb.Deserialize(cm.Int32TType, row[0])
	if err != nil || id.(int32) != 1 {
		t.Errorf("Expected id=1, got %v", id)
	}

	name, err := flimsydb.Deserialize(cm.StringTType, row[1])
	if err != nil || name.(string) != "Initial" {
		t.Errorf("Expected name='Initial', got %v", name)
	}

	updateValues := map[string]any{
		"name": "Updated",
	}
	if err := table.UpdateRow(0, updateValues); err != nil {
		t.Fatalf("Error updating row: %v", err)
	}

	updatedRow, err := table.GetRow(0)
	if err != nil {
		t.Fatalf("Error getting updated row: %v", err)
	}

	updatedName, err := flimsydb.Deserialize(cm.StringTType, updatedRow[1])
	if err != nil || updatedName.(string) != "Updated" {
		t.Errorf("Expected name='Updated', got %v", updatedName)
	}
}

func TestDeleteRow(t *testing.T) {
	col1, err := flimsydb.NewColumn("id", cm.Int32TType, int32(0), indexer.AbsentIndexerType, 0)
	if err != nil {
		t.Fatalf("Failed to create column 'id': %v", err)
	}

	columns := []*flimsydb.Column{col1}
	table := flimsydb.NewTable(columns)

	for i := int32(0); i < 3; i++ {
		if err := table.InsertRow(map[string]any{"id": i}); err != nil {
			t.Fatalf("Error inserting row %d: %v", i, err)
		}
	}

	if err := table.DeleteRow(1); err != nil {
		t.Fatalf("Error deleting row: %v", err)
	}

	row0, err := table.GetRow(0)
	if err != nil {
		t.Fatalf("Error getting first row: %v", err)
	}

	id0, err := flimsydb.Deserialize(cm.Int32TType, row0[0])
	if err != nil || id0.(int32) != 0 {
		t.Errorf("Expected id=0, got %v", id0)
	}

	row1, err := table.GetRow(1)
	if err != nil {
		t.Fatalf("Error getting second row: %v", err)
	}

	id2, err := flimsydb.Deserialize(cm.Int32TType, row1[0])
	if err != nil || id2.(int32) != 2 {
		t.Errorf("Expected id=2, got %v", id2)
	}
}
