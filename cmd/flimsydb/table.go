package flimsydb

import (
	"fmt"
	"sync"
)

type Row []Blob

type Table struct {
	mu          sync.RWMutex
	Columns     []*Column
	ColumnIndex map[string]int
	Rows        []Row
}

func NewTable(columns []*Column) *Table {
	columnIndex := make(map[string]int, len(columns))
	for i, col := range columns {
		columnIndex[col.Name] = i
	}

	return &Table{
		Columns:     columns,
		ColumnIndex: columnIndex,
		Rows:        []Row{},
	}
}

func (t *Table) validateTypes(vals map[string]any) error {
	t.mu.RLock()
	defer t.mu.RUnlock()

	for colName, colVal := range vals {
		colIndex, exists := t.ColumnIndex[colName]
		if !exists {
			return fmt.Errorf("column '%s': %w", colName, ErrColumnNotFound)
		}

		col := t.Columns[colIndex]
		if err := validateType(colVal, col.Type); err != nil {
			return fmt.Errorf("column '%s': %w", col.Name, ErrTypeMismatch)
		}
	}

	return nil
}

func (t *Table) indexInBounds(index int) error {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if index < 0 || index >= len(t.Rows) {
		return ErrIndexOutOfBounds
	}

	return nil
}

func (t *Table) InsertRow(values map[string]any) error {
	if err := t.validateTypes(values); err != nil {
		return fmt.Errorf("validation failed: %w", err)
	}

	t.mu.RLock()
	row := make(Row, len(t.Columns))

	for i, col := range t.Columns {
		value, exists := values[col.Name]

		var blobValue Blob
		var err error
		if !exists {
			blobValue = col.Default
		} else {
			blobValue, err = Serialize(col.Type, value)
			if err != nil {
				t.mu.RUnlock()
				return fmt.Errorf("serialization failed: %w", ErrInvalidData)
			}
		}

		row[i] = blobValue
	}
	t.mu.RUnlock()

	t.mu.Lock()
	defer t.mu.Unlock()
	t.Rows = append(t.Rows, row)

	return nil
}

func (t *Table) GetRow(index int) (Row, error) {
	if err := t.indexInBounds(index); err != nil {
		return nil, err
	}

	t.mu.RLock()
	defer t.mu.RUnlock()

	result := make(Row, len(t.Columns))
	copy(result, t.Rows[index])

	return result, nil
}

func (t *Table) UpdateRow(index int, values map[string]any) error {
	if err := t.indexInBounds(index); err != nil {
		return err
	}

	if err := t.validateTypes(values); err != nil {
		return fmt.Errorf("validation failed: %w", err)
	}

	t.mu.RLock()
	row := make(Row, len(t.Columns))
	copy(row, t.Rows[index])

	for colName, newValue := range values {
		colIndex := t.ColumnIndex[colName]
		col := t.Columns[colIndex]

		blobValue, err := Serialize(col.Type, newValue)
		if err != nil {
			t.mu.RUnlock()
			return fmt.Errorf("serialization failed: %w", ErrInvalidData)
		}

		row[colIndex] = blobValue
	}
	t.mu.RUnlock()

	t.mu.Lock()
	defer t.mu.Unlock()
	t.Rows[index] = row

	return nil
}

func (t *Table) DeleteRow(index int) error {
	if err := t.indexInBounds(index); err != nil {
		return err
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	t.Rows = append(t.Rows[:index], t.Rows[index+1:]...)

	return nil
}
