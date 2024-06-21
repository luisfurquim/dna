package dnaoracle

import (
	"fmt"
	"reflect"
	"database/sql/driver"
)

func (s *Scanner) Scan(parms ...interface{})  error {
	var row []driver.Value
	var i int
	var val reflect.Value
	var err error

	row = make([]driver.Value, len(parms))

	err = s.rows.Next(row)
	if err!=nil {
		return err
	}

	Goose.Query.Logf(5,"row: %#v", row)
	for i, _ = range row {
		Goose.Query.Logf(6,"col: %#v", row[i])
		if row[i] != nil {
			Goose.Query.Logf(6,"!nil")
			val = reflect.ValueOf(parms[i]).Elem()
			switch val.Kind() {
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				Goose.Query.Logf(5,"int")
				fmt.Sscanf(row[i].(string), "%d", val.Addr().Interface())
				Goose.Query.Logf(5,"%s->%d", row[i], val.Interface())
			case reflect.Float32, reflect.Float64:
				Goose.Query.Logf(5,"real")
				fmt.Sscanf(row[i].(string), "%f", val.Addr().Interface())
			case reflect.Bool:
				Goose.Query.Logf(5,"bool")
				val.Set(reflect.ValueOf(row[i].(string) == "T"))
			default:
				Goose.Query.Logf(5,"any")
				val.Set(reflect.ValueOf(row[i]).Convert(val.Type()))
			}
		}
	}

	Goose.Query.Logf(5,"end scan")

	return nil
}