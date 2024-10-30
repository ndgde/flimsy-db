package main

import (
	"fmt"
	"log"

	fdb "github.com/ndgde/flimsy-db/cmd/flimsydb"
	"github.com/ndgde/flimsy-db/cmd/flimsydb/indexer"
)

func main() {
	// Initialize DB
	db := fdb.NewFlimsyDB()
	fmt.Println("🗄️  Initializing FlimsyDB...")

	// Create table
	columns := []*fdb.Column{
		fdb.NewColumn("id", fdb.Int32ColumnType, fdb.NewInt32Tabular(0), indexer.HashMapIndexerType),
		fdb.NewColumn("name", fdb.StringColumnType, fdb.NewStringTabular(""), indexer.AbsentIndexerType),
		fdb.NewColumn("balance", fdb.Float64ColumnType, fdb.NewFloat64Tabular(0.0), indexer.HashMapIndexerType),
	}

	if err := db.CreateTable("accounts", columns); err != nil {
		log.Fatal(err)
	}
	fmt.Println("✓ Table 'accounts' created")

	table, err := db.GetTable("accounts")
	if err != nil {
		log.Fatal(err)
	}

	// Insert sample data
	accounts := []map[string]any{
		{"id": int32(1), "name": "Alice", "balance": float64(1000.50)},
		{"id": int32(2), "name": "Bob", "balance": float64(2500.75)},
		{"id": int32(3), "name": "Charlie", "balance": float64(750.25)},
	}

	fmt.Println("\n📝 Inserting accounts:")
	for _, acc := range accounts {
		if err := table.InsertRow(acc); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("✓ Added: ID=%v, Name=%v, Balance=$%.2f\n",
			acc["id"], acc["name"], acc["balance"])
	}

	// Read data
	fmt.Println("\n📖 Reading accounts:")
	for i := 0; i < len(accounts); i++ {
		row, err := table.GetRow(i)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("Row %d: ID=%v, Name=%v, Balance=$%.2f\n",
			i, row["id"], row["name"], row["balance"])
	}

	// Update data
	fmt.Println("\n📊 Updating balance for first account:")
	updateData := map[string]any{"balance": float64(1500.00)}
	if err := table.UpdateRow(0, updateData); err != nil {
		log.Fatal(err)
	}

	// Read updated data
	row, err := table.GetRow(0)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("✓ Updated: ID=%v, Name=%v, Balance=$%.2f\n",
		row["id"], row["name"], row["balance"])
}
