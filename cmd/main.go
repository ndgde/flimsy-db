package main

import (
	"fmt"

	"math/rand"

	fdb "github.com/ndgde/flimsy-db/cmd/flimsydb"
	cm "github.com/ndgde/flimsy-db/cmd/flimsydb/common"
	"github.com/ndgde/flimsy-db/cmd/flimsydb/indexer"
)

func main() {
	col1, err := fdb.NewColumn("Name", cm.StringTType, "", indexer.BTreeIndexerType, 0)
	if err != nil {
		fmt.Println(err)
	}

	col2, err := fdb.NewColumn("Age", cm.Int32TType, int32(0), indexer.HashMapIndexerType, 0)
	if err != nil {
		fmt.Println(err)
	}

	col3, err := fdb.NewColumn("Salary", cm.Float64TType, float64(0), indexer.BTreeIndexerType, 0)
	if err != nil {
		fmt.Println(err)
	}

	col4, err := fdb.NewColumn("Position", cm.StringTType, "", indexer.HashMapIndexerType, 0)
	if err != nil {
		fmt.Println(err)
	}

	col5, err := fdb.NewColumn("Department", cm.StringTType, "", indexer.BTreeIndexerType, 0)
	if err != nil {
		fmt.Println(err)
	}

	col6, err := fdb.NewColumn("Experience", cm.Int32TType, int32(0), indexer.AbsentIndexerType, 0)
	if err != nil {
		fmt.Println(err)
	}

	col7, err := fdb.NewColumn("Country", cm.StringTType, "", indexer.HashMapIndexerType, 0)
	if err != nil {
		fmt.Println(err)
	}

	columns := []*fdb.Column{col1, col2, col3, col4, col5, col6, col7}

	table := fdb.NewTable(columns)

	for i := 0; i < 250; i++ {
		row := map[string]any{
			"Name":       randomName(),
			"Age":        int32(rand.Intn(65) + 18),
			"Salary":     float64(rand.Intn(100000)) / 100.0,
			"Position":   randomPosition(),
			"Department": randomDepartment(),
			"Experience": int32(rand.Intn(40)),
			"Country":    randomCountry(),
		}

		if err := table.InsertRow(row); err != nil {
			fmt.Println("Row adding error:", err)
		}
	}

	fdb.PrintTable(table)

	rows, err := table.Find("Name", "Jane Smith")
	if err != nil {
		fmt.Printf("finding error: %v", err)
	}
	for _, row := range rows {
		fmt.Println(row)
	}

	// start := time.Now()
	// rows, err = table.FindInRange("Age", int32(19), int32(22))
	// elapsed := time.Since(start)
	// if err != nil {
	// 	fmt.Printf("finding error: %v", err)
	// }
	// fmt.Printf("Execution time: %s\n", elapsed)
	// for _, row := range rows {
	// 	fmt.Println(row)
	// }
	// fmt.Println()

	// start = time.Now()
	// rows, err = table.FindInRange("Experience", int32(20), int32(22))
	// elapsed = time.Since(start)
	// if err != nil {
	// 	fmt.Printf("finding error: %v", err)
	// }
	// fmt.Printf("Execution time: %s\n", elapsed)
	// for _, row := range rows {
	// 	fmt.Println(row)
	// }
}

func randomName() string {
	names := []string{"John Doe", "Max Mustermann", "Fill Murray", "Jane Smith", "Alice Johnson", "Bob Brown"}
	return names[rand.Intn(len(names))]
}

func randomPosition() string {
	positions := []string{"Engineer", "Manager", "Analyst", "Director", "Consultant", "Developer"}
	return positions[rand.Intn(len(positions))]
}

func randomDepartment() string {
	departments := []string{"Finance", "Engineering", "HR", "Sales", "Marketing", "Support"}
	return departments[rand.Intn(len(departments))]
}

func randomCountry() string {
	countries := []string{"USA", "Germany", "Canada", "UK", "Australia", "India"}
	return countries[rand.Intn(len(countries))]
}
