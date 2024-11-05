package flimsydb

type TabularType int

const (
	StringTType TabularType = iota
	Int32TType
	Float64TType
)

type Blob []byte /* Binary Large OBject */
