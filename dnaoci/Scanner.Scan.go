package dnaoci

import (
	"fmt"
	"io"
	"reflect"
)

func (s *Scanner) Scan(parms ...interface{}) error {
	if !s.rows.Next() {
		if err := s.rows.Err(); err != nil {
			return err
		}
		return io.EOF
	}

	// Use database/sql's built-in scanning for simple types
	// For complex types (bool encoded as "T"/"F"), we scan as string and convert
	dest := make([]interface{}, len(parms))
	needsConvert := make([]bool, len(parms))

	for i, p := range parms {
		val := reflect.ValueOf(p).Elem()
		switch val.Kind() {
		case reflect.Bool:
			// Oracle stores bool as CHAR(1): "T"/"F"
			var tmp string
			dest[i] = &tmp
			needsConvert[i] = true
		default:
			dest[i] = p
		}
	}

	err := s.rows.Scan(dest...)
	if err != nil {
		Goose.Query.Logf(1, "Scan error: %s", err)
		return err
	}

	// Post-process conversions
	for i, conv := range needsConvert {
		if conv {
			val := reflect.ValueOf(parms[i]).Elem()
			tmp := dest[i].(*string)
			val.Set(reflect.ValueOf(*tmp == "T"))
		}
	}

	var out string
	for i, v := range parms {
		out += fmt.Sprintf("%d:%#v ", i, reflect.ValueOf(v).Elem().Interface())
	}
	Goose.Query.Logf(5, "end scan -> parms: %s", out)

	return nil
}
