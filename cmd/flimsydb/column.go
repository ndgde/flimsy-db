package flimsydb

import (
	idxr "github.com/ndgde/flimsy-db/cmd/flimsydb/indexer"
)

type ColumnType int

const (
	StringColumnType ColumnType = iota
	Int32ColumnType
	Float64ColumnType
)

type Column struct {
	Name    string
	Type    ColumnType
	Default any
	Idxr    any
}

func NewColumn(name string, valType ColumnType, defaultVal any, idxrType idxr.IndexerType) *Column {
	var indexer any
	switch valType {
	case Int32ColumnType:
		indexer = idxr.NewIndexer[idxr.IndexableInt32](idxrType)
	case Float64ColumnType:
		indexer = idxr.NewIndexer[idxr.IndexableFloat64](idxrType)
	case StringColumnType:
		indexer = idxr.NewIndexer[idxr.IndexableString](idxrType)
	}

	return &Column{
		Name:    name,
		Type:    valType,
		Default: defaultVal,
		Idxr:    indexer,
	}
}
