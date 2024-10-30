package flimsydb

type Serializable interface {
	Serialize() ([]byte, error)
	Deserialize(data []byte) error
}

type Tabular interface {
	Serializable
	GetValue() any
}

type Orderable interface {
	Equal(other any) bool
	Less(other any) bool
	Greater(other any) bool
	Convert(value any) (any, error)
}

type Indexer interface {
	Add(val any, ptr int) error
	Delete(val any, ptr int) error
	Update(oldVal, newVal any, ptr int) error
	Find(val any) ([]int, bool)
	FindInRange(min, max any) ([]int, bool)
}

func NewInt32Tabular(value int32) *Int32Tabular {
	return &Int32Tabular{Value: value}
}

func NewFloat64Tabular(value float64) *Float64Tabular {
	return &Float64Tabular{Value: value}
}

func NewStringTabular(value string) *StringTabular {
	return &StringTabular{Value: value}
}
