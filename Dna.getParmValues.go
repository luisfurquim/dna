package dna

import (
	"reflect"
)

func (d *Dna) getParmValues(tabName string, refRow reflect.Value, recursive func(row interface{})) (PK, []interface{}) {
	var fld field
	var parms []interface{}
	var fk reflect.Value
	var id PK

	parms = make([]interface{}, 0, len(d.tables[tabName].fields))
	for _, fld = range d.tables[tabName].fields {
//		Goose.Query.Logf(0, "fld:%#v", fld)
		if fld.joinList { //aqui
			fk = refRow.Field(fld.index)
			if isNonEmptySlice(fk) {
			}
			continue
		}

		if fld.fk != "" {
//			Goose.Query.Logf(0, "fld.fk:%#v", fld.fk)
			fk = refRow.Field(fld.index)
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
				
				fk = fk.Elem().Field(d.tables[fld.fk].pkIndex)
//				Goose.Query.Logf(0, "++fk:%#v", fk)
				parms = append(parms, fk.Interface())
			}
		} else {
			parms = append(parms, refRow.Field(fld.index).Interface())
//			Goose.Query.Logf(0, "parms:%#v", parms)
		}
	}

	if d.tables[tabName].pkName != "" {
		id = refRow.Field(d.tables[tabName].pkIndex).Interface().(PK)
	}

	return id, parms
}
