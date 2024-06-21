package dna

import (
	"reflect"
)

func (d *Dna) getParmValues(tabName string, refRow reflect.Value, recursive func(row interface{})) (PK, []interface{}) {
	var fld FieldSpec
	var parms []interface{}
	var fk reflect.Value
	var id PK

	parms = make([]interface{}, 0, len(d.tables[tabName].fields))
	for _, fld = range d.tables[tabName].fields {
//		Goose.Query.Logf(0, "fld:%#v", fld)
		if fld.JoinList { //aqui
			fk = refRow.Field(fld.Index)
			if isNonEmptySlice(fk) {
			}
			continue
		}

		if fld.Fk != "" {
//			Goose.Query.Logf(0, "fld.fk:%#v", fld.fk)
			fk = refRow.Field(fld.Index)
//			Goose.Query.Logf(0, "fk:%#v", fk)
			if !fk.IsValid() || fk.IsNil() || fk.IsZero() {
//				Goose.Query.Logf(0, "!valid")
				parms = append(parms, 0)
			} else {
//				Goose.Query.Logf(0, "recursive?")
				if recursive != nil {
//					Goose.Query.Logf(0, "yes")
					recursive(fk.Interface())
				}
				
				fk = fk.Elem().Field(d.tables[fld.Fk].pkIndex)
//				Goose.Query.Logf(0, "++fk:%#v", fk)
				parms = append(parms, fk.Interface())
			}
		} else {
			if !fld.PK {
				parms = append(parms, refRow.Field(fld.Index).Interface())
			}
//			Goose.Query.Logf(0, "parms:%#v", parms)
		}
	}

	if d.tables[tabName].pkName != "" {
		id = refRow.Field(d.tables[tabName].pkIndex).Interface().(PK)
	}

	return id, parms
}
