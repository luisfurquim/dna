package dna

import (
	"reflect"
	"database/sql/driver"
)

func (d *Dna) getParmValues(tabName string, refRow reflect.Value, recursive func(row interface{})) (string, PK, []driver.NamedValue) {
	var fld FieldSpec
	var parms []driver.NamedValue
	var fk reflect.Value
	var id PK
	var pkName string

	parms = make([]driver.NamedValue, 0, len(d.tables[tabName].fields))
	for _, fld = range d.tables[tabName].fields {
		Goose.Query.Logf(6, "fld:%#v", fld)
		if fld.JoinList { //aqui
			fk = refRow.Field(fld.Index)
//			if isNonEmptySlice(fk) {
//			}
			continue
		}

		if fld.Fk != "" {
			Goose.Query.Logf(5, "fld.fk:%#v", fld.Fk)
			fk = refRow.Field(fld.Index)
			Goose.Query.Logf(5, "fk:%#v", fk)
			if !fk.IsValid() || fk.IsNil() || fk.IsZero() {
				Goose.Query.Logf(5, "!valid")
				parms = append(parms, driver.NamedValue{
					Name: fld.Name,
					Value: 0,
				})
			} else {
				Goose.Query.Logf(5, "recursive?")
				if recursive != nil {
					Goose.Query.Logf(5, "yes")
					recursive(fk.Interface())
				}
				
				fk = fk.Elem().Field(d.tables[fld.Fk].pkIndex)
				Goose.Query.Logf(5, "++fk:%#v", fk)
				parms = append(parms, driver.NamedValue{
					Name: fld.Name,
					Value:  fk.Interface(),
				})
			}
		} else {
			if fld.PK {
				id = refRow.Field(fld.Index).Interface().(PK)
				pkName = fld.Name
			} else {
				parms = append(parms, driver.NamedValue{
					Name: fld.Name,
					Value: refRow.Field(fld.Index).Interface(),
				})
			}
			Goose.Query.Logf(6, "parms:%#v", parms)
		}
	}

	Goose.Query.Logf(5, "parms:%#v", parms)

	return pkName, id, parms
}
