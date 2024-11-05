package flimsydb

type Column struct {
	Name    string
	Type    TabularType
	Default Blob
}

func NewColumn(name string, valType TabularType, defaultVal any) (*Column, error) {
	if err := validateType(defaultVal, valType); err != nil {
		return nil, err
	}

	blobDefaultVal, err := Serialize(valType, defaultVal)
	if err != nil {
		return nil, err
	}

	return &Column{
		Name:    name,
		Type:    valType,
		Default: blobDefaultVal,
	}, nil
}
