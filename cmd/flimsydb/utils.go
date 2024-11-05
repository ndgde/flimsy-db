package flimsydb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"reflect"
	"strings"
)

func validateType(val any, expType TabularType) error {
	var isValid bool
	var typeName string

	switch expType {
	case StringTType:
		_, isValid = val.(string)
		typeName = "string"
	case Int32TType:
		_, isValid = val.(int32)
		typeName = "int32"
	case Float64TType:
		_, isValid = val.(float64)
		typeName = "float64"
	default:
		return errors.New("type is not tabular")
	}

	if !isValid {
		return fmt.Errorf("expected %s type but got %s", typeName, reflect.TypeOf(val).String())
	}
	return nil
}

func Serialize(valueType TabularType, value any) (Blob, error) {
	buf := new(bytes.Buffer)

	switch valueType {
	case Int32TType:
		v, ok := value.(int32)
		if !ok {
			return nil, errors.New("value does not match int32 type")
		}
		if err := binary.Write(buf, binary.LittleEndian, v); err != nil {
			return nil, err
		}

	case Float64TType:
		v, ok := value.(float64)
		if !ok {
			return nil, errors.New("value does not match float64 type")
		}
		if err := binary.Write(buf, binary.LittleEndian, v); err != nil {
			return nil, err
		}

	case StringTType:
		v, ok := value.(string)
		if !ok {
			return nil, errors.New("value does not match string type")
		}
		strBytes := []byte(v)
		strLen := int32(len(strBytes))
		if err := binary.Write(buf, binary.LittleEndian, strLen); err != nil {
			return nil, err
		}
		if _, err := buf.Write(strBytes); err != nil {
			return nil, err
		}

	default:
		return nil, errors.New("unknown data type")
	}

	return buf.Bytes(), nil
}

func Deserialize(valueType TabularType, value Blob) (any, error) {
	buf := bytes.NewReader(value)

	switch valueType {
	case Int32TType:
		var v int32
		if err := binary.Read(buf, binary.LittleEndian, &v); err != nil {
			return nil, err
		}
		return v, nil

	case Float64TType:
		var v float64
		if err := binary.Read(buf, binary.LittleEndian, &v); err != nil {
			return nil, err
		}
		return v, nil

	case StringTType:
		var strLen int32
		if err := binary.Read(buf, binary.LittleEndian, &strLen); err != nil {
			return nil, err
		}
		strBytes := make([]byte, strLen)
		if _, err := buf.Read(strBytes); err != nil {
			return nil, err
		}
		return string(strBytes), nil

	default:
		return nil, errors.New("unknown data type")
	}
}

func getColumnWidths(t *Table) []int {
	widths := make([]int, len(t.Columns))

	for i, col := range t.Columns {
		widths[i] = len(col.Name)
	}

	for _, row := range t.Rows {
		for i, col := range t.Columns {
			value, err := Deserialize(col.Type, row[i])
			if err != nil {
				continue
			}
			width := len(fmt.Sprintf("%v", value))
			if width > widths[i] {
				widths[i] = width
			}
		}
	}

	return widths
}

func PrintTable(t *Table) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if len(t.Columns) == 0 {
		fmt.Println("Empty table")
		return
	}

	widths := getColumnWidths(t)

	printLine := func() {
		fmt.Print("+")
		for _, w := range widths {
			fmt.Print(strings.Repeat("-", w+2) + "+")
		}
		fmt.Println()
	}

	printLine()
	fmt.Print("|")
	for i, col := range t.Columns {
		fmt.Printf(" %-*s |", widths[i], col.Name)
	}
	fmt.Println()
	printLine()

	for _, row := range t.Rows {
		fmt.Print("|")
		for i, col := range t.Columns {
			value, err := Deserialize(col.Type, row[i])
			if err != nil {
				fmt.Printf(" %-*s |", widths[i], "ERROR")
				continue
			}

			switch col.Type {
			case Float64TType:
				fmt.Printf(" %*.*f |", widths[i], 2, value)
			case Int32TType:
				fmt.Printf(" %*v |", widths[i], value)
			default:
				fmt.Printf(" %-*v |", widths[i], value)
			}
		}
		fmt.Println()
	}

	printLine()
}
