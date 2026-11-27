package integrationkey

import (
	"database/sql/driver"
	"fmt"
)

// Type is the entity that needs an integration.
type Type string

// TypeGeneric is the only kind of integration key: alerts arrive through the
// generic API (/api/v2/generic/incoming).
const TypeGeneric Type = "generic"

func (s Type) Value() (driver.Value, error) {
	str := string(s)
	return str, nil
}

func (s *Type) Scan(value interface{}) error {
	switch t := value.(type) {
	case []byte:
		*s = Type(t)
	case string:
		*s = Type(t)
	default:
		return fmt.Errorf("could not process unknown type for source %T", t)
	}
	return nil
}
