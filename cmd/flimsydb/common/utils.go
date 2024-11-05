package common

import (
	"bytes"
	"encoding/binary"
	"math"
)

type CompareFunc func(a, b []byte) int

func GetCompareFunc(typ TabularType) CompareFunc {
	switch typ {
	case Int32TType:
		return compareInt32
	// case fdb.Int64TType:
	// 	return compareInt64
	case Float64TType:
		return compareFloat64
	case StringTType:
		return compareString
	// case fdb.BoolTType:
	// 	return compareBool
	default:
		return nil
	}
}

func compareInt32(a, b []byte) int {
	valA := int32(binary.BigEndian.Uint32(a))
	valB := int32(binary.BigEndian.Uint32(b))
	if valA < valB {
		return -1
	}
	if valA > valB {
		return 1
	}
	return 0
}

func compareInt64(a, b []byte) int {
	valA := int64(binary.BigEndian.Uint64(a))
	valB := int64(binary.BigEndian.Uint64(b))
	if valA < valB {
		return -1
	}
	if valA > valB {
		return 1
	}
	return 0
}

func compareFloat64(a, b []byte) int {
	valA := math.Float64frombits(binary.BigEndian.Uint64(a))
	valB := math.Float64frombits(binary.BigEndian.Uint64(b))
	if valA < valB {
		return -1
	}
	if valA > valB {
		return 1
	}
	return 0
}

func compareString(a, b []byte) int {
	return bytes.Compare(a, b)
}

func compareBool(a, b []byte) int {
	valA := a[0] != 0
	valB := b[0] != 0
	if valA == valB {
		return 0
	}
	if !valA && valB {
		return -1
	}
	return 1
}

func Equal(a, b []byte, compareFunc CompareFunc) bool {
	return compareFunc(a, b) == 0
}

func Less(a, b []byte, compareFunc CompareFunc) bool {
	return compareFunc(a, b) < 0
}

func LessOrEqual(a, b []byte, compareFunc CompareFunc) bool {
	return compareFunc(a, b) <= 0
}

func Greater(a, b []byte, compareFunc CompareFunc) bool {
	return compareFunc(a, b) > 0
}

func GreaterOrEqual(a, b []byte, compareFunc CompareFunc) bool {
	return compareFunc(a, b) >= 0
}
