package dna

import (
	"reflect"
)

func (d *Dna) save(row interface{}, visited map[string]struct{}, opt []SaveOption) (PK, error) {
	var id int64
	var i, j int
	var pk PK
	var tabName string
	var refTabName string
	var refRow reflect.Value
	var err error
	var parms []interface{}
	var fname string
	var fk reflect.Value
	var related reflect.Type
	var ok bool
	var noCascade bool
	var o SaveOption

//	Goose.Query.Logf(0, ">>>>>>> opts: %#v", opt)
	if len(opt) > 0 {
		for _, o = range opt {
			if o == NoCascade {
//				Goose.Query.Logf(0, "NoCascade")
				noCascade = true
			}
		}
	}

	tabName, refRow, err = d.getSingleRefs(row)
	if err != nil {
		return 0, err
	}

	visited[tabName] = struct{}{}

//	Goose.Query.Logf(0, "tabName: %s, refRow: %#v", tabName, refRow)

	if noCascade {
		pk, parms = d.getParmValues(tabName, refRow, func(r interface{}) {})
	} else {
		pk, parms = d.getParmValues(tabName, refRow, func(r interface{}) {
//			Goose.Query.Logf(0, "r: %#v", r)
			// recursively save related tables
			d.save(r, visited, opt)
		})
	}

//	Goose.Query.Logf(0, "id:%d, parms: %#v", id, parms)

	if pk==0 {
//		Goose.Query.Logf(0, "d.insert:%#v, tabName: %s", d.insert, tabName)
		id, err = d.insert[tabName].Insert(parms...)
		if err != nil {
			Goose.Query.Logf(1, "Insert error on %s: %s", tabName, err)
			return 0, err
		}
		if d.tables[tabName].pkName != "" {
			refRow.Field(d.tables[tabName].pkIndex).SetInt(id)
		}
		pk = PK(id)
	} else {
		parms = append(parms, pk)
		Goose.Query.Logf(5, "-=-=-=-=-=-=-=-=-=-=-=-=-=- Update parms on %s: %#v", tabName, parms)
		err = d.updateBy[tabName]["id"].Exec(parms...)
//		Goose.Query.Logf(1, "Update error %s on %s: %#v", err, tabName, parms)
		if err != nil {
			Goose.Query.Logf(1, "Update error on %s: %s", tabName, err)
			return 0, err
		}
	}

//	Goose.Query.Logf(1, "d.tables[tabName].xrefs: %#v", d.tables[tabName].xrefs)

	if noCascade {
		return pk, nil
	}

	Goose.Query.Logf(3, "Cascading save of %s", tabName)

	for refTabName, fname = range d.tables[tabName].xrefs {
		if _, ok = visited[refTabName]; ok {
			continue
		}
		fk = refRow.FieldByName(fname)
//		Goose.Query.Logf(1, "fk:%#v", fk)
		if fk.IsValid() && (!fk.IsNil()) && (!fk.IsZero()) {
			related = fk.Type().Elem()
			if related.Kind() == reflect.Pointer {
				related = related.Elem()
			}
//			Goose.Query.Logf(1, "related:%#v", related)
//			Goose.Query.Logf(1, "fk.Elem():%#v", fk.Index(0).Interface())
//			Goose.Query.Logf(1, "refRow:%#v", refRow.Interface())
			for i=0; i<related.NumField(); i++ {
				if related.Field(i).Type.Kind() != reflect.Ptr {
					continue
				}
//				Goose.Query.Logf(1, "related.Field(i).Type:%#v", related.Field(i).Type.Elem())
//				Goose.Query.Logf(1, "refRow.Type():%#v", refRow.Type())
//				Goose.Query.Logf(1, "fk.Elem():%#v", fk.Index(0).Field(i).Interface())
//				Goose.Query.Logf(1, "refRow:%#v", refRow.Interface())
				if related.Field(i).Type.Elem() == refRow.Type() {
					for j=0; j<fk.Len(); j++ {
						fk.Index(j).Elem().Field(i).Set(refRow.Addr())
//						Goose.Query.Logf(1, "fk.Index(j).Elem().Field(i):%#v", fk.Index(j).Elem().Field(i))
						d.save(fk.Index(j).Interface(), visited, opt)
					}
					break
				}
			}
		}
	}

//	Goose.Query.Logf(0, "******************** Saved: %#v", refRow.Interface())

	return pk, nil
}


func (d *Dna) Save(row interface{}, opt ...SaveOption) (PK, error) {
	return d.save(row, map[string]struct{}{}, opt)
}
