package tests

import (
	"testing"

	fdb "github.com/ndgde/flimsy-db/cmd/flimsydb"
)

func TestTable_InsertRow(t *testing.T) {
	columns := []fdb.Column{
		{Name: "ID", Type: fdb.IntType, Default: 0},
		{Name: "Name", Type: fdb.StringType, Default: "Unknown"},
		{Name: "Age", Type: fdb.IntType, Default: 18},
		{Name: "Salary", Type: fdb.FloatType, Default: 0.0},
	}

	table := fdb.NewTable(columns)

	err := table.InsertRow(map[string]any{"ID": 1, "Name": "Alice", "Age": 30})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	err = table.InsertRow(map[string]any{"ID": "invalid", "Name": "Charlie"})
	if err == nil {
		t.Fatalf("expected error, got none")
	}
}

func TestTable_GetRow(t *testing.T) {
	columns := []fdb.Column{
		{Name: "ID", Type: fdb.IntType, Default: 0},
		{Name: "Name", Type: fdb.StringType, Default: "Unknown"},
		{Name: "Age", Type: fdb.IntType, Default: 18},
		{Name: "Salary", Type: fdb.FloatType, Default: 0.0},
	}

	table := fdb.NewTable(columns)

	table.InsertRow(map[string]any{"ID": 1, "Name": "Alice", "Age": 30})

	row, err := table.GetRow(0)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if row["Name"] != "Alice" || row["Age"] != 30 {
		t.Fatalf("expected Name: Alice, Age: 30, got Name: %v, Age: %v", row["Name"], row["Age"])
	}

	_, err = table.GetRow(1)
	if err == nil {
		t.Fatalf("expected error, got none")
	}
}

func TestTable_UpdateRow(t *testing.T) {
	columns := []fdb.Column{
		{Name: "ID", Type: fdb.IntType, Default: 0},
		{Name: "Name", Type: fdb.StringType, Default: "Unknown"},
		{Name: "Salary", Type: fdb.FloatType, Default: 0.0},
	}

	table := fdb.NewTable(columns)

	table.InsertRow(map[string]any{"ID": 1, "Name": "Alice"})

	err := table.UpdateRow(0, map[string]any{"Name": "Bob", "Salary": 1000.50})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	row, err := table.GetRow(0)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if row["Name"] != "Bob" || row["Salary"] != 1000.50 {
		t.Fatalf("expected Name: Bob, Salary: 1000.50, got Name: %v, Salary: %v", row["Name"], row["Salary"])
	}
}

func TestTable_DeleteRow(t *testing.T) {
	columns := []fdb.Column{
		{Name: "ID", Type: fdb.IntType, Default: 0},
		{Name: "Name", Type: fdb.StringType, Default: "Unknown"},
	}

	table := fdb.NewTable(columns)

	table.InsertRow(map[string]any{"ID": 1, "Name": "Alice"})

	err := table.DeleteRow(0)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	_, err = table.GetRow(0)
	if err == nil {
		t.Fatalf("expected error, got none")
	}

	err = table.DeleteRow(0)
	if err == nil {
		t.Fatalf("expected error, got none")
	}
}

func TestTable_Concurrency(t *testing.T) {
	columns := []fdb.Column{
		{Name: "ID", Type: fdb.IntType, Default: 0},
		{Name: "Name", Type: fdb.StringType, Default: "Unknown"},
	}

	table := fdb.NewTable(columns)

	for i := 0; i < 100; i++ {
		err := table.InsertRow(map[string]any{"ID": i, "Name": "User"})
		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}
	}

	for i := 0; i < 100; i++ {
		row, err := table.GetRow(i)
		if err != nil && i < len(table.Rows) {
			t.Errorf("expected no error, got %v", err)
		} else if i < len(table.Rows) && row["ID"] != i {
			t.Errorf("expected ID: %d, got ID: %v", i, row["ID"])
		}
	}
}
