package main

import (
	"fmt"

	fdb "github.com/ndgde/flimsy-db/cmd/flimsydb"
)

func main() {
	col1, err := fdb.NewColumn("Name", fdb.StringTType, "")
	if err != nil {
		fmt.Println(err)
	}

	col2, err := fdb.NewColumn("Age", fdb.Int32TType, int32(0))
	if err != nil {
		fmt.Println(err)
	}

	col3, err := fdb.NewColumn("Salary", fdb.Float64TType, float64(0))
	if err != nil {
		fmt.Println(err)
	}

	columns := []*fdb.Column{col1, col2, col3}

	table := fdb.NewTable(columns)

	if err = table.InsertRow(map[string]any{
		"Name":   "John Doe",
		"Age":    int32(32),
		"Salary": float64(12),
	}); err != nil {
		fmt.Println("Row adding error :", err)
	}

	if err = table.InsertRow(map[string]any{
		"Name":   "Max Mustermann",
		"Age":    int32(25),
		"Salary": float64(103),
	}); err != nil {
		fmt.Println("Row adding error :", err)
	}

	if err = table.InsertRow(map[string]any{
		"Name":   "Fill Murray",
		"Age":    int32(70),
		"Salary": float64(75),
	}); err != nil {
		fmt.Println("Row adding error :", err)
	}

	fdb.PrintTable(table)
}
