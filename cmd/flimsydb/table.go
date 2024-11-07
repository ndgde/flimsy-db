package flimsydb

import (
	"fmt"
	"sync"

	cm "github.com/ndgde/flimsy-db/cmd/flimsydb/common"
	"github.com/ndgde/flimsy-db/cmd/flimsydb/indexer"
)

type Row []cm.Blob

type Table struct {
	mu          sync.RWMutex
	scheme      Scheme
	columnIndex map[string]int
	rows        []Row
	// rowMutexes  map[int]sync.RWMutex
}

func NewTable(scheme Scheme) *Table {
	columnIndex := make(map[string]int, len(scheme))
	for i, col := range scheme {
		columnIndex[col.Name] = i
	}

	return &Table{
		scheme:      scheme,
		columnIndex: columnIndex,
		rows:        []Row{},
	}
}

func (t *Table) validateTypes(vals map[string]any) error {
	t.mu.RLock()
	defer t.mu.RUnlock()

	for colName, colVal := range vals {
		colIndex, exists := t.columnIndex[colName]
		if !exists {
			return fmt.Errorf("column '%s': %w", colName, cm.ErrColumnNotFound)
		}

		col := t.scheme[colIndex]
		if err := validateType(colVal, col.Type); err != nil {
			return fmt.Errorf("column '%s': %w", col.Name, cm.ErrTypeMismatch)
		}
	}

	return nil
}

func (t *Table) indexInBounds(index int) error {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if index < 0 || index >= len(t.rows) {
		return cm.ErrIndexOutOfBounds
	}

	return nil
}

func (t *Table) InsertRow(values map[string]any) error {
	if err := t.validateTypes(values); err != nil {
		return fmt.Errorf("validation failed: %w", err)
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	row := make(Row, len(t.scheme))

	for i, col := range t.scheme {
		value, exists := values[col.Name]

		var blobValue cm.Blob
		var err error
		if !exists {
			blobValue = col.Default
		} else {
			blobValue, err = Serialize(col.Type, value)
			if err != nil {
				return fmt.Errorf("serialization failed: %w", cm.ErrInvalidData)
			}
		}

		if col.Flags&UniqueFlag != 0 {
			rows, err := t.Find(col.Name, value)
			if err != nil {
				return fmt.Errorf("error when checking value for uniqueness")
			}
			if len(rows) != 0 {
				return fmt.Errorf("the field contains the \"unique\" flag, but the supplied value %v already exists", value)
			}
		}

		row[i] = blobValue
	}

	if err := IdxrAddRow(t.scheme, row, len(t.rows)); err != nil {
		return fmt.Errorf("indexation failed during add: %w", err)
	}

	t.rows = append(t.rows, row)

	return nil
}

func (t *Table) GetRow(index int) (Row, error) {
	if err := t.indexInBounds(index); err != nil {
		return nil, err
	}

	t.mu.RLock()
	defer t.mu.RUnlock()

	rowCopy := CopyRow(t.rows[index])

	return rowCopy, nil
}

func (t *Table) UpdateRow(index int, values map[string]any) error {
	if err := t.indexInBounds(index); err != nil {
		return err
	}

	if err := t.validateTypes(values); err != nil {
		return fmt.Errorf("validation failed: %w", err)
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	oldRow := CopyRow(t.rows[index])
	newRow := CopyRow(t.rows[index])

	for colName, newValue := range values {
		colIndex := t.columnIndex[colName]
		col := t.scheme[colIndex]

		blobValue, err := Serialize(col.Type, newValue)
		if err != nil {
			return fmt.Errorf("serialization failed: %w", cm.ErrInvalidData)
		}

		if col.Flags&ImmutableFlag != 0 {
			return fmt.Errorf("error when trying to change a field marked with the \"immutable\" flag")
		}

		if col.Flags&UniqueFlag != 0 {
			rows, err := t.Find(col.Name, newValue)
			if err != nil {
				return fmt.Errorf("error when checking value for uniqueness")
			}
			if cm.Equal(newRow[colIndex], blobValue, cm.GetCompareFunc(col.Type)) {
				if len(rows) != 1 {
					return fmt.Errorf("the field contains the \"unique\" flag, but the supplied value %v already exists", newValue)
				}
			} else if len(rows) != 0 {
				return fmt.Errorf("the field contains the \"unique\" flag, but the supplied value %v already exists", newValue)
			}
		}

		newRow[colIndex] = blobValue
	}

	if err := IdxrUpdateRow(t.scheme, oldRow, newRow, index); err != nil {
		return fmt.Errorf("indexation failed during update: %w", err)
	}

	t.rows[index] = newRow

	return nil
}

func (t *Table) DeleteRow(index int) error {
	if err := t.indexInBounds(index); err != nil {
		return err
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	oldRow := CopyRow(t.rows[index])

	if err := IdxrDeleteRow(t.scheme, oldRow, index); err != nil {
		return fmt.Errorf("indexation failed during delete: %w", err)
	}

	for i := index + 1; i < len(t.rows); i++ {
		row := t.rows[i]
		if err := IdxrDeleteRow(t.scheme, row, i); err != nil {
			return fmt.Errorf("indexation failed during update: %w", err)
		}

		if err := IdxrAddRow(t.scheme, row, i-1); err != nil {
			return fmt.Errorf("indexation failed during re-add: %w", err)
		}
	}

	t.rows = append(t.rows[:index], t.rows[index+1:]...)

	return nil
}

func (t *Table) RestoreIndexing() error {
	for _, col := range t.scheme {
		col.Idxr = indexer.NewIndexer(col.IdxrType, col.Type)
	}

	t.mu.RLock()
	defer t.mu.RUnlock()
	for i, row := range t.rows {
		if err := IdxrAddRow(t.scheme, row, i); err != nil {
			return fmt.Errorf("re-indexing failed at row %d: %w", i, err)
		}
	}

	return nil
}

func (t *Table) Find(colName string, val any) ([][]any, error) {
	colIndex, exists := t.columnIndex[colName]
	if !exists {
		return nil, fmt.Errorf("column with name %q does not exist", colName)
	}

	col := t.scheme[colIndex]

	if err := validateType(val, col.Type); err != nil {
		return nil, fmt.Errorf("validation error: %w", err)
	}

	blobValue, err := Serialize(col.Type, val)
	if err != nil {
		return nil, fmt.Errorf("value serialization error: %w", err)
	}

	var indexes []int
	if col.IdxrType == indexer.AbsentIndexerType {
		t.mu.RLock()
		if col.Flags&UniqueFlag != 0 {
			for i, row := range t.rows {
				if cm.Equal(row[colIndex], blobValue, cm.GetCompareFunc(col.Type)) {
					indexes = append(indexes, i)
					break
				}
			}
		} else {
			for i, row := range t.rows {
				if cm.Equal(row[colIndex], blobValue, cm.GetCompareFunc(col.Type)) {
					indexes = append(indexes, i)
				}
			}
		}
		t.mu.RUnlock()
	} else {
		indexes = col.Idxr.Find(blobValue)
	}

	t.mu.RLock()
	defer t.mu.RUnlock()
	result := make([][]any, len(indexes))
	for i, rowIndex := range indexes {
		result[i], err = DeserializeRow(t.scheme, CopyRow(t.rows[rowIndex]))
		if err != nil {
			return nil, fmt.Errorf("row deserialization error: %w", err)
		}
	}

	return result, nil
}

func (t *Table) FindInRange(colName string, minVal any, maxVal any) ([][]any, error) {
	colIndex, exists := t.columnIndex[colName]
	if !exists {
		return nil, fmt.Errorf("column with name %q does not exist", colName)
	}

	col := t.scheme[colIndex]

	if err := validateType(minVal, col.Type); err != nil {
		return nil, fmt.Errorf("validation error: %w", err)
	}

	if err := validateType(maxVal, col.Type); err != nil {
		return nil, fmt.Errorf("validation error: %w", err)
	}

	blobMinVal, err := Serialize(col.Type, minVal)
	if err != nil {
		return nil, fmt.Errorf("value serialization error: %w", err)
	}

	blobMaxVal, err := Serialize(col.Type, maxVal)
	if err != nil {
		return nil, fmt.Errorf("value serialization error: %w", err)
	}

	var indexes []int
	if col.IdxrType == indexer.AbsentIndexerType || col.IdxrType == indexer.HashMapIndexerType {
		t.mu.RLock()
		for i, row := range t.rows {
			compFunc := cm.GetCompareFunc(col.Type)
			if cm.LessOrEqual(row[colIndex], blobMaxVal, compFunc) && cm.GreaterOrEqual(row[colIndex], blobMinVal, compFunc) {
				indexes = append(indexes, i)
			}
		}
		t.mu.RUnlock()
	} else {
		indexes = col.Idxr.FindInRange(blobMinVal, blobMaxVal)
	}

	t.mu.RLock()
	defer t.mu.RUnlock()
	result := make([][]any, len(indexes))
	for i, rowIndex := range indexes {
		result[i], err = DeserializeRow(t.scheme, CopyRow(t.rows[rowIndex]))
		if err != nil {
			return nil, fmt.Errorf("row deserialization error: %w", err)
		}
	}

	return result, nil
}
