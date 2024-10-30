package flimsydb

import (
	"bytes"
	"encoding/binary"
	"math"

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
	Default Tabular
	Idxr    any
}

func NewColumn(name string, valType ColumnType, defaultVal Tabular, idxrType idxr.IndexerType) *Column {
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

func ToTabular(value any) (Tabular, error) {
	switch v := value.(type) {
	case int32:
		return NewInt32Tabular(v), nil

	case float64:
		return NewFloat64Tabular(v), nil

	case string:
		return NewStringTabular(v), nil

	default:
		return nil, ErrInappropriateType
	}
}

type StringTabular struct{ Value string }

func (s *StringTabular) GetValue() any { return s.Value }
func (s *StringTabular) Serialize() ([]byte, error) {
	return []byte(s.Value), nil
}
func (s *StringTabular) Deserialize(data []byte) error {
	s.Value = string(data)
	return nil
}

type Int32Tabular struct{ Value int32 }

func (i *Int32Tabular) GetValue() any { return i.Value }
func (i *Int32Tabular) Serialize() ([]byte, error) {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.LittleEndian, i.Value)
	return buf.Bytes(), err
}
func (i *Int32Tabular) Deserialize(data []byte) error {
	if len(data) != 4 {
		return ErrInvalidData
	}
	i.Value = int32(binary.LittleEndian.Uint32(data))
	return nil
}

type Float64Tabular struct{ Value float64 }

func (f *Float64Tabular) GetValue() any { return f.Value }
func (f *Float64Tabular) Serialize() ([]byte, error) {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.LittleEndian, f.Value)
	return buf.Bytes(), err
}
func (f *Float64Tabular) Deserialize(data []byte) error {
	if len(data) != 8 {
		return ErrInvalidData
	}
	bits := binary.LittleEndian.Uint64(data)
	f.Value = math.Float64frombits(bits)
	return nil
}
