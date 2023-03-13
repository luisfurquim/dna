package dna

import (
	"reflect"
)

func (d *Dna) getParmPointers(tabName string, refRow reflect.Value) (int64, []interface{}) {
	var fld field
	var parms []interface{}

	parms = make([]interface{}, 0, len(d.tables[tabName].fields))
	for _, fld = range d.tables[tabName].fields {
		if !fld.joinList {
			parms = append(parms, refRow.Field(fld.index).Addr().Interface())
		}
	}

	return refRow.Field(d.tables[tabName].pkIndex).Interface().(int64), parms
}
