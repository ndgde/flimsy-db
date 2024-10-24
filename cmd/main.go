package main

import (
	"fmt"
	"log"

	fdb "github.com/ndgde/flimsy-db/cmd/flimsydb"
)

// func check(err error) {
// 	if err != nil {
// 		log.Fatal(err)
// 	}
// }

func main() {
	columns := []fdb.Column{
		{Name: "ID", Type: fdb.Int, Default: 0},
		{Name: "Name", Type: fdb.String, Default: "Unknown"},
		{Name: "Age", Type: fdb.Int, Default: 18},
		{Name: "Salary", Type: fdb.Float, Default: 0.0},
	}

	table := fdb.NewTable(columns)

	table.InsertRow(map[string]any{"ID": 1, "Name": "Alice", "Age": 30})
	table.InsertRow(map[string]any{"ID": 2})
	table.InsertRow(map[string]any{"ID": 3, "Name": "Bob", "Age": 25, "Salary": 1000.50})

	table.PrintTable()

	if err := table.UpdateRow(1, map[string]any{"Name": "Charlie", "Age": 28}); err != nil {
		log.Fatal(err)
	}

	if err := table.DeleteRow(0); err != nil {
		log.Fatal(err)
	}

	table.PrintTable()

	if row, err := table.GetRow(0); err == nil {
		fmt.Println("Содержимое первой строки:")
		for key, value := range row {
			fmt.Printf("%s: %v\n", key, value)
		}
	} else {
		fmt.Println(err)
	}
}
