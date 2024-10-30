package flimsydb

import "errors"

var (
	ErrTableExists           = errors.New("table already exists")
	ErrTableNotFound         = errors.New("table not found")
	ErrColumnNotFound        = errors.New("column not found")
	ErrTypeMismatch          = errors.New("type mismatch")
	ErrIndexOutOfBounds      = errors.New("index out of bounds")
	ErrInvalidKey            = errors.New("invalid key")
	ErrUnsupportedType       = errors.New("unsupported type")
	ErrConversionFailed      = errors.New("failed to convert to/from bytes")
	ErrInappropriateType     = errors.New("inappropriate type for column")
	ErrInvalidData           = errors.New("invalid data format")
	ErrSerializationFailed   = errors.New("serialization failed")
	ErrDeserializationFailed = errors.New("deserialization failed")
	ErrRowDeleted            = errors.New("row deleted")
)
