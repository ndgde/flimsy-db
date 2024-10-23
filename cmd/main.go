package main

import (
	"fmt"
	"log"

	"github.com/ndgde/flimsy-db/cmd/flimsydb"
)

func check(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func main() {
	db := flimsydb.NewFlimsyDB()

	err := db.Create("foo", "bar")
	check(err)

	value, err := db.Read("foo")
	check(err)
	fmt.Println("Value:", value)

	err = db.Update("foo", "baz")
	check(err)

	value, err = db.Read("foo")
	check(err)
	fmt.Println("Updated value:", value)

	err = db.Delete("foo")
	check(err)

	_, err = db.Read("foo")
	if err != nil {
		fmt.Println("Error:", err)
	}
}
