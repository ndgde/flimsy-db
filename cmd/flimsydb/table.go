package flimsydb

import (
	"fmt"
	"sync"
)

type Row []Tabular

type Table struct {
	mu          sync.RWMutex
	Columns     []*Column
	Rows        [][]any
	rowMutexes  []*sync.RWMutex
	deletedRows map[int]bool
}

func NewTable(columns []*Column) *Table {
	return &Table{
		Columns:     columns,
		Rows:        make([][]any, 0),
		rowMutexes:  make([]*sync.RWMutex, 0),
		deletedRows: make(map[int]bool),
	}
}

func (t *Table) validateValues(values map[string]any) error {
	t.mu.RLock()
	defer t.mu.RUnlock()

	columnMap := make(map[string]*Column)
	for _, col := range t.Columns {
		columnMap[col.Name] = col
	}

	for key, value := range values {
		column, exists := columnMap[key]
		if !exists {
			return ErrInvalidKey
		}

		switch column.Type {
		case Int32ColumnType:
			if _, ok := value.(int32); !ok {
				return fmt.Errorf("%w: expected int32 for column %s", ErrInappropriateType, key)
			}
		case Float64ColumnType:
			if _, ok := value.(float64); !ok {
				return fmt.Errorf("%w: expected float64 for column %s", ErrInappropriateType, key)
			}
		case StringColumnType:
			if _, ok := value.(string); !ok {
				return fmt.Errorf("%w: expected string for column %s", ErrInappropriateType, key)
			}
		}
	}

	return nil
}

func (t *Table) validateTabularValue(value any, columnType ColumnType) error {
	switch columnType {
	case Int32ColumnType:
		if _, ok := value.(*Int32Tabular); !ok {
			return ErrInappropriateType
		}
	case Float64ColumnType:
		if _, ok := value.(*Float64Tabular); !ok {
			return ErrInappropriateType
		}
	case StringColumnType:
		if _, ok := value.(*StringTabular); !ok {
			return ErrInappropriateType
		}
	default:
		return ErrUnsupportedType
	}
	return nil
}

func (t *Table) InsertRow(values map[string]any) error {
	if err := t.validateValues(values); err != nil {
		return err
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	row := make([]any, len(t.Columns))
	rowIndex := len(t.Rows)

	for i, column := range t.Columns {
		value, exists := values[column.Name]
		if !exists {
			value = column.Default.GetValue()
		}

		tabularValue, err := ToTabular(value)
		if err != nil {
			return err
		}

		if err := t.validateTabularValue(tabularValue, column.Type); err != nil {
			return err
		}

		if indexer, ok := column.Idxr.(Indexer); ok {
			if err := indexer.Add(tabularValue.GetValue(), rowIndex); err != nil {
				return err
			}
		}

		row[i] = tabularValue
	}

	t.Rows = append(t.Rows, row)
	t.rowMutexes = append(t.rowMutexes, new(sync.RWMutex))
	return nil
}

func (t *Table) GetRow(index int) (map[string]any, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if index < 0 || index >= len(t.Rows) {
		return nil, ErrIndexOutOfBounds
	}

	if t.deletedRows[index] {
		return nil, ErrRowDeleted
	}

	t.rowMutexes[index].RLock()
	defer t.rowMutexes[index].RUnlock()

	result := make(map[string]any)
	for i, value := range t.Rows[index] {
		tabular, ok := value.(Tabular)
		if !ok {
			return nil, ErrInappropriateType
		}
		result[t.Columns[i].Name] = tabular.GetValue()
	}

	return result, nil
}

func (t *Table) UpdateRow(index int, values map[string]any) error {
	if err := t.validateValues(values); err != nil {
		return err
	}

	t.mu.RLock()
	if index < 0 || index >= len(t.Rows) {
		t.mu.RUnlock()
		return ErrIndexOutOfBounds
	}

	t.rowMutexes[index].Lock()
	t.mu.RUnlock()
	defer t.rowMutexes[index].Unlock()

	row := make([]any, len(t.Columns))
	copy(row, t.Rows[index])

	for i, col := range t.Columns {
		if newValue, exists := values[col.Name]; exists {
			tabularValue, err := ToTabular(newValue)
			if err != nil {
				return err
			}

			if indexer, ok := col.Idxr.(Indexer); ok {
				oldTabular := t.Rows[index][i].(Tabular)
				if err := indexer.Update(oldTabular.GetValue(), tabularValue.GetValue(), index); err != nil {
					return err
				}
			}

			row[i] = tabularValue
		} else {
			row[i] = t.Rows[index][i]
		}
	}

	t.Rows[index] = row
	return nil
}

func (t *Table) DeleteRow(index int) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if index < 0 || index >= len(t.Rows) {
		return ErrIndexOutOfBounds
	}

	if t.deletedRows[index] {
		return ErrRowDeleted
	}

	for i, col := range t.Columns {
		if indexer, ok := col.Idxr.(Indexer); ok {
			tabular := t.Rows[index][i].(Tabular)
			if err := indexer.Delete(tabular.GetValue(), index); err != nil {
				t.mu.RUnlock()
				return err
			}
		}
	}

	t.deletedRows[index] = true
	return nil
}

func (t *Table) PrintTable() {
	t.mu.RLock()
	defer t.mu.RUnlock()

	for _, column := range t.Columns {
		fmt.Printf("%s\t", column.Name)
	}
	fmt.Println()

	for i, row := range t.Rows {
		t.rowMutexes[i].RLock()
		for _, value := range row {
			fmt.Printf("%v\t", value)
		}
		fmt.Println()
		t.rowMutexes[i].RUnlock()
	}
}

func (t *Table) Cleanup() {
	t.mu.Lock()
	defer t.mu.Unlock()

	// First clear all existing indexes
	for i, col := range t.Columns {
		if indexer, ok := col.Idxr.(Indexer); ok {
			for oldIndex, row := range t.Rows {
				if !t.deletedRows[oldIndex] {
					tabular := row[i].(Tabular)
					_ = indexer.Delete(tabular.GetValue(), oldIndex)
				}
			}
		}
	}

	// Collect non-deleted rows while preserving their values
	newRows := make([][]any, 0, len(t.Rows))
	newMutexes := make([]*sync.RWMutex, 0, len(t.Rows))
	for oldIndex, row := range t.Rows {
		if !t.deletedRows[oldIndex] {
			newRows = append(newRows, row)
			newMutexes = append(newMutexes, t.rowMutexes[oldIndex])
		}
	}

	// Add values to indexes with new positions
	for newIndex, row := range newRows {
		for i, col := range t.Columns {
			if indexer, ok := col.Idxr.(Indexer); ok {
				tabular := row[i].(Tabular)
				_ = indexer.Add(tabular.GetValue(), newIndex)
			}
		}
	}

	t.Rows = newRows
	t.rowMutexes = newMutexes
	t.deletedRows = make(map[int]bool)
}
