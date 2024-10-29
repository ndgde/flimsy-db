package main

import (
	"fmt"
	"log"

	idxr "github.com/ndgde/flimsy-db/cmd/flimsydb/indexer"
)

type MyKey int

func (k MyKey) Less(other MyKey) bool {
	return k < other
}

func (k MyKey) Greater(other MyKey) bool {
	return k > other
}

func (k MyKey) LessOrEqual(other MyKey) bool {
	return k <= other
}

func (k MyKey) GreaterOrEqual(other MyKey) bool {
	return k >= other
}

func main() {
	indexer := idxr.NewHashMapIndexer[MyKey]()

	err := indexer.Add(1, 100)
	if err != nil {
		log.Fatalf("Error adding: %v", err)
	}

	err = indexer.Add(1, 101)
	if err != nil {
		log.Fatalf("Error adding: %v", err)
	}

	ptrs, notFound := indexer.Find(1)
	if notFound {
		fmt.Println("Value not found")
	} else {
		fmt.Printf("Found pointers for 1: %v\n", ptrs)
	}

	err = indexer.Update(1, 1, 100)
	if err != nil {
		log.Fatalf("Error updating: %v", err)
	}

	ptrs, notFound = indexer.Find(2)
	if notFound {
		fmt.Println("Value not found")
	} else {
		fmt.Printf("Found pointers for 2: %v\n", ptrs)
	}

	err = indexer.Delete(2, 100)
	if err != nil {
		fmt.Printf("Error deleting: %v\n", err)
	}

	_, notFound = indexer.Find(2)
	if notFound {
		fmt.Println("Value not found")
	}
}
