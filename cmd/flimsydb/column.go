package flimsydb

import (
	cm "github.com/ndgde/flimsy-db/cmd/flimsydb/common"
	"github.com/ndgde/flimsy-db/cmd/flimsydb/indexer"
)

type Column struct {
	Name     string
	Type     cm.TabularType
	Default  cm.Blob
	IdxrType indexer.IndexerType
	Idxr     indexer.Indexer
}

type Scheme []*Column

func NewColumn(name string, valType cm.TabularType, defaultVal any, idxrType indexer.IndexerType) (*Column, error) {
	if err := validateType(defaultVal, valType); err != nil {
		return nil, err
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
