package main

import (
	"fmt"

	fdb "github.com/ndgde/flimsy-db/cmd/flimsydb"
)

// func check(err error) {
// 	if err != nil {
// 		log.Fatal(err)
// 	}
// }

func main() {
	db := fdb.NewFlimsyDB()

	columns := []fdb.Column{
		{Name: "ID", Type: fdb.IntType, Default: 0},
		{Name: "Name", Type: fdb.StringType, Default: ""},
		{Name: "Age", Type: fdb.IntType, Default: 0},
	}

	err := db.CreateTable("Users", columns)
	if err != nil {
		fmt.Println("Error creating table:", err)
		return
	}

	table, err := db.GetTable("Users")
	if err != nil {
		fmt.Println("Error getting table:", err)
		return
	}

	err = table.InsertRow(map[string]any{"ID": 1, "Name": "Alice", "Age": 30})
	if err != nil {
		fmt.Println("Error inserting row:", err)
	}

	err = table.InsertRow(map[string]any{"ID": 2, "Name": "Bob", "Age": 25})
	if err != nil {
		fmt.Println("Error inserting row:", err)
	}

	fmt.Println("Users Table:")
	table.PrintTable()

	row, err := table.GetRow(0)
	if err != nil {
		fmt.Println("Error getting row:", err)
	} else {
		fmt.Println("First row:", row)
	}

	err = table.UpdateRow(0, map[string]any{"Age": 31})
	if err != nil {
		fmt.Println("Error updating row:", err)
	}

	fmt.Println("Users Table after update:")
	table.PrintTable()

	err = table.DeleteRow(1)
	if err != nil {
		fmt.Println("Error deleting row:", err)
	}

	fmt.Println("Users Table after deletion:")
	table.PrintTable()
}
