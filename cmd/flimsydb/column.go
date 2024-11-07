package flimsydb

import (
	"fmt"

	cm "github.com/ndgde/flimsy-db/cmd/flimsydb/common"
	"github.com/ndgde/flimsy-db/cmd/flimsydb/indexer"
)

type FlagsType int

const (
	UniqueFlag FlagsType = 1 << iota
	NotNullFlag
	PrimaryKeyFlag
	ForeignKeyFlag
	ImmutableFlag
)

type Column struct {
	Name     string
	Type     cm.TabularType
	Default  cm.Blob
	IdxrType indexer.IndexerType
	Idxr     indexer.Indexer
	Flags    FlagsType
}

type Scheme []*Column

func NewColumn(name string, valType cm.TabularType, defaultVal any, idxrType indexer.IndexerType, flags FlagsType) (*Column, error) {
	if err := validateType(defaultVal, valType); err != nil {
		return nil, err
	}

	/* flag validation */
	if flags&PrimaryKeyFlag != 0 && flags&ForeignKeyFlag != 0 {
		return nil, fmt.Errorf("flags error: a field cannot be both a primary and a foreign key")
	}
	if flags&PrimaryKeyFlag != 0 {
		flags |= UniqueFlag | NotNullFlag
	}
	if flags&ForeignKeyFlag != 0 && flags&NotNullFlag == 0 {
		flags |= NotNullFlag
	}

	blobDefaultVal, err := Serialize(valType, defaultVal)
	if err != nil {
		return nil, err
	}

	return &Column{
		Name:     name,
		Type:     valType,
		Default:  blobDefaultVal,
		IdxrType: idxrType,
		Idxr:     indexer.NewIndexer(idxrType, valType),
	}, nil
}
