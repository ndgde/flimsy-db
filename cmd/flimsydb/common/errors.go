package common

import "errors"

var (
	// Table errors
	ErrTableExists   = errors.New("table already exists")
	ErrTableNotFound = errors.New("table not found")

	// Column errors
	ErrColumnNotFound = errors.New("column not found")

	// Data errors
	ErrTypeMismatch = errors.New("value type does not match column type")
	ErrInvalidData  = errors.New("invalid data provided")

	// Index errors
	ErrIndexOutOfBounds = errors.New("row index out of bounds")
	ErrIndexExists      = errors.New("index already exists")
	ErrIndexNotFound    = errors.New("index not found")
)
