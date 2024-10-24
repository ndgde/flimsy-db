package flimsydb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sync"
)

type ColumnType int

const (
	StringType ColumnType = iota
	IntType
	FloatType
)

type Column struct {
	Name    string
	Type    ColumnType
	Default any
}

var (
	ErrIndexOutOfBounds  = errors.New("index out of bounds")
	ErrUnsupportedType   = errors.New("unsupported type")
	ErrInappropriateType = errors.New("inappropriate type")
	ErrInvalidKey        = errors.New("invalid key")
	ErrConversionFailed  = errors.New("failed to convert to/from bytes")
)

type Row [][]byte

type Table struct {
	mu      sync.RWMutex
	Columns []Column
	Rows    []Row
}

func NewTable(columns []Column) *Table {
	return &Table{
		Columns: columns,
		Rows:    []Row{},
	}
}

func toBytes(value any) ([]byte, error) {
	var buf bytes.Buffer
	var data []byte

	switch v := value.(type) {
	case int:
		data = make([]byte, 8)
		binary.LittleEndian.PutUint64(data, uint64(v))

	case float64:
		data = make([]byte, 8)
		binary.LittleEndian.PutUint64(data, math.Float64bits(v))

	case string:
		data = []byte(v)

	default:
		return nil, ErrUnsupportedType
	}

	if _, err := buf.Write(data); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func fromBytes(data []byte, colType ColumnType) (any, error) {
	switch colType {
	case IntType:
		if len(data) < 8 {
			return nil, ErrConversionFailed
		}
		val := int64(binary.LittleEndian.Uint64(data))
		return int(val), nil

	case FloatType:
		if len(data) < 8 {
			return nil, ErrConversionFailed
		}
		bits := binary.LittleEndian.Uint64(data)
		return math.Float64frombits(bits), nil

	case StringType:
		return string(data), nil

	default:
		return nil, ErrConversionFailed
	}
}

func (t *Table) validateValues(values map[string]any) error {
	for key, value := range values {
		columnFound := false
		for _, column := range t.Columns {
			if column.Name == key {
				columnFound = true
				switch column.Type {
				case IntType:
					if _, ok := value.(int); !ok {
						return ErrInappropriateType
					}
				case FloatType:
					if _, ok := value.(float64); !ok {
						return ErrInappropriateType
					}
				case StringType:
					if _, ok := value.(string); !ok {
						return ErrInappropriateType
					}
				}
				break
			}
		}
		if !columnFound {
			return ErrInvalidKey
		}
	}

	return nil
}

func (t *Table) InsertRow(values map[string]any) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if err := t.validateValues(values); err != nil {
		return err
	}

	var row Row
	for _, column := range t.Columns {
		value, exists := values[column.Name]
		if !exists {
			value = column.Default
		}

		data, err := toBytes(value)
		if err != nil {
			return err
		}

		row = append(row, data)
	}
	t.Rows = append(t.Rows, row)

	return nil
}

func (t *Table) GetRow(index int) (map[string]any, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if index < 0 || index >= len(t.Rows) {
		return nil, ErrIndexOutOfBounds
	}

	var rowCopy Row
	copy(rowCopy[:], t.Rows[index][:])

	result := make(map[string]any)
	for i, byteValue := range t.Rows[index] {
		value, err := fromBytes(byteValue, t.Columns[i].Type)
		if err != nil {
			return nil, err
		}

		result[t.Columns[i].Name] = value
	}

	return result, nil
}

/*
For now, due to this error, this data operation is not atomic and
can be performed partially, this should be corrected in the future
*/
func (t *Table) UpdateRow(index int, values map[string]any) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if index < 0 || index >= len(t.Rows) {
		return ErrIndexOutOfBounds
	}

	if err := t.validateValues(values); err != nil {
		return err
	}

	for valIndex := range t.Rows[index] {
		if newValue, exists := values[t.Columns[valIndex].Name]; exists {
			byteValue, err := toBytes(newValue)
			if err != nil {
				return err
			}

			t.Rows[index][valIndex] = byteValue
		}
	}

	return nil
}

func (t *Table) DeleteRow(index int) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if index < 0 || index >= len(t.Rows) {
		return ErrIndexOutOfBounds
	}

	t.Rows = append(t.Rows[:index], t.Rows[index+1:]...) /* is still a very expensive operation */

	return nil
}

func (t *Table) PrintTable() {
	t.mu.RLock()
	defer t.mu.RUnlock()

	for _, column := range t.Columns {
		fmt.Printf("%s\t", column.Name)
	}
	fmt.Println()

	for _, row := range t.Rows {
		for i, byteValue := range row {
			value, err := fromBytes(byteValue, t.Columns[i].Type)

			if err != nil {
				value = "Err"
			}

			fmt.Printf("%v\t", value)
		}

		fmt.Println()
	}
}
