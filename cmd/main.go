package main

import (
	"fmt"
	"log"

	fdb "github.com/ndgde/flimsy-db/cmd/flimsydb"
)

type MyKey struct {
	value int
}

func (k MyKey) Less(other int) bool {
	return k.value < other
}

func (k MyKey) Greater(other int) bool {
	return k.value > other
}

func (k MyKey) LessOrEqual(other int) bool {
	return k.value <= other
}

func (k MyKey) GreaterOrEqual(other int) bool {
	return k.value >= other
}

func main() {
	indexer := fdb.NewHashMapIndexer[int, MyKey]()

	err := indexer.Add(MyKey{value: 1}, 100)
	if err != nil {
		log.Fatalf("Error adding: %v", err)
	}

	err = indexer.Add(MyKey{value: 1}, 101)
	if err != nil {
		log.Fatalf("Error adding: %v", err)
	}

	err = indexer.Add(MyKey{value: 1}, 100)
	if err != nil {
		fmt.Println(err)
	}

	ptrs, notFound := indexer.Find(MyKey{value: 1})
	if notFound {
		fmt.Println("Value not found")
	} else {
		fmt.Printf("Found pointers for 1: %v\n", ptrs)
	}

	err = indexer.Update(MyKey{value: 1}, MyKey{value: 2}, 100)
	if err != nil {
		log.Fatalf("Error updating: %v", err)
	}

	ptrs, notFound = indexer.Find(MyKey{value: 2})
	if notFound {
		fmt.Println("Value not found")
	} else {
		fmt.Printf("Found pointers for 2: %v\n", ptrs)
	}

	err = indexer.Delete(MyKey{value: 2}, 100)
	if err != nil {
		log.Fatalf("Error deleting: %v", err)
	}

	_, notFound = indexer.Find(MyKey{value: 2})
	if notFound {
		fmt.Println("Value not found")
	}
}
