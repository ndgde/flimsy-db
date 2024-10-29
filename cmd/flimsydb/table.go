package flimsydb

import (
	"errors"
	"fmt"
	"sync"
)

var (
	ErrIndexOutOfBounds  = errors.New("index out of bounds")
	ErrUnsupportedType   = errors.New("unsupported type")
	ErrInappropriateType = errors.New("inappropriate type")
	ErrInvalidKey        = errors.New("invalid key")
	ErrConversionFailed  = errors.New("failed to convert to/from bytes")
)

type Serializable interface {
	Serialize() ([]byte, error)
	Deserialize(data []byte) error
}

type Tabular interface {
	Serializable
}

type Row []Tabular

type Table struct {
	mu      sync.RWMutex
	Columns []*Column
	Rows    []Row
}

func NewTable(columns []*Column) *Table {
	return &Table{
		Columns: columns,
		Rows:    []Row{},
	}
}

/*
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
*/

func (t *Table) validateValues(values map[string]any) error {
	for key, value := range values {
		columnFound := false
		for _, column := range t.Columns {
			if column.Name == key {
				columnFound = true
				switch column.Type {
				case Int32ColumnType:
					if _, ok := value.(int32); !ok {
						return ErrInappropriateType
					}
				case Float64ColumnType:
					if _, ok := value.(float64); !ok {
						return ErrInappropriateType
					}
				case StringColumnType:
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

		row = append(row, value)
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
	for i, value := range t.Rows[index] {
		result[t.Columns[i].Name] = value
	}

	return result, nil
}

func (t *Table) UpdateRow(index int, values map[string]any) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if index < 0 || index >= len(t.Rows) {
		return ErrIndexOutOfBounds
	}

	if err := t.validateValues(values); err != nil {
		return err
	}

	rowSize := len(t.Columns)
	row := make(Row, rowSize)
	for valIndex := range t.Rows[index] {
		if newValue, exists := values[t.Columns[valIndex].Name]; exists {
			row[valIndex] = newValue

		} else {
			row[valIndex] = t.Rows[index][valIndex]
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
		for _, value := range row {
			fmt.Printf("%v\t", value)
		}

		fmt.Println()
	}
}
