package main

import (
	"io"
	"os"
	"strings"
	"testing"

	fdb "github.com/ndgde/flimsy-db/cmd/flimsydb"
)

func TestCreateTable(t *testing.T) {
	db := fdb.NewFlimsyDB()
	columns := []fdb.Column{
		{Name: "ID", Type: fdb.IntType},
		{Name: "Name", Type: fdb.StringType},
	}

	err := db.CreateTable("TestTable", columns)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if !db.TableExists("TestTable") {
		t.Fatalf("expected table to exist")
	}
}

func TestCreateTableExists(t *testing.T) {
	db := fdb.NewFlimsyDB()
	columns := []fdb.Column{
		{Name: "ID", Type: fdb.IntType},
	}

	_ = db.CreateTable("TestTable", columns)

	err := db.CreateTable("TestTable", columns)
	if err == nil || err != fdb.ErrTableExists {
		t.Fatalf("expected error %v, got %v", fdb.ErrTableExists, err)
	}
}

func TestGetTable(t *testing.T) {
	db := fdb.NewFlimsyDB()
	columns := []fdb.Column{
		{Name: "ID", Type: fdb.IntType},
	}

	_ = db.CreateTable("TestTable", columns)

	table, err := db.GetTable("TestTable")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if table == nil {
		t.Fatal("expected table to be non-nil")
	}
}

func TestGetTableNotFound(t *testing.T) {
	db := fdb.NewFlimsyDB()

	_, err := db.GetTable("NonExistentTable")
	if err == nil || err != fdb.ErrTableNotFound {
		t.Fatalf("expected error %v, got %v", fdb.ErrTableNotFound, err)
	}
}

func TestDeleteTable(t *testing.T) {
	db := fdb.NewFlimsyDB()
	columns := []fdb.Column{
		{Name: "ID", Type: fdb.IntType},
	}

	_ = db.CreateTable("TestTable", columns)

	err := db.Delete("TestTable")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if db.TableExists("TestTable") {
		t.Fatalf("expected table to be deleted")
	}
}

func TestDeleteTableNotFound(t *testing.T) {
	db := fdb.NewFlimsyDB()

	err := db.Delete("NonExistentTable")
	if err == nil || err != fdb.ErrTableNotFound {
		t.Fatalf("expected error %v, got %v", fdb.ErrTableNotFound, err)
	}
}
func TestPrintTables(t *testing.T) {
	db := fdb.NewFlimsyDB()
	columns := []fdb.Column{
		{Name: "ID", Type: fdb.IntType},
	}

	_ = db.CreateTable("TestTable", columns)

	oldStdout := os.Stdout
	defer func() { os.Stdout = oldStdout }()

	r, w, _ := os.Pipe()
	os.Stdout = w

	go func() {
		defer w.Close()
		db.PrintTables()
	}()

	var out strings.Builder
	_, _ = io.Copy(&out, r)

	output := out.String()
	if !strings.Contains(output, "TestTable") {
		t.Fatal("expected output to contain 'TestTable'")
	}
}
