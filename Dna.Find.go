package dna

import (
	"strings"
	"reflect"
   "github.com/gwenn/gosqlite"
)

func (d *Dna) getList(tabName, rule string) (l *list, err error) {
	var ok bool
	var r map[string]*list

	if r, ok = d.list[tabName]; !ok {
		Goose.Query.Logf(1, "Error listing table %s: %s", tabName, ErrNoTablesFound)
		err = ErrNoTablesFound
		return
	}

	if l, ok = r[rule]; !ok {
		Goose.Query.Logf(1, "Error listing table %s: %s", tabName, ErrRuleNotFound)
//		Goose.Query.Logf(1, "rule %s: rules %#v", rule, d.list)
		err = ErrRuleNotFound
		return
	}

	return
}

func (d *Dna) nextRow(tabName string, l *list, row reflect.Value, s *sqlite.Stmt, outerBy map[string]interface{}, typ reflect.Type, getField func(reflect.Value, int) reflect.Value) error {
	var parms []interface{}
	var i, c int
	var fld, target reflect.Value
	var ok bool
	var fkTabName string
	var fkTable table
//	var fkList *list
	var lst tabRule
//	var fkrule tabRule
	var fkField field
	var tName string
	var err error
	var related reflect.Value
	var relatedRow reflect.Value
	var fldName string
	var opt []string
	var fkeys map[int]interface{}
	var frows map[int]reflect.Value
	var fkey interface{}
	var pkIndex PK
	var pkIndexPtr interface{}
	var by map[string]interface{}

	parms = make([]interface{}, len(l.cols))
	fkeys = map[int]interface{}{}
	frows = map[int]reflect.Value{}

	by = map[string]interface{}{}
	for k, v := range outerBy {
		by[k] = v
	}


//	Goose.Query.Logf(0,"====================================================================== l.joins: %#v", l.joins)

	// allocate the scan parameters
	for i, c = range l.cols {
		fld = getField(row, c)
		if fld.Type().Kind() == reflect.Pointer {
			fld.Set(reflect.New(fld.Type().Elem()))
			tName = d.tableType[fld.Elem().Type().Name()]
			parms[i] = fld.Elem().Field(d.tables[tName].pkIndex).Addr().Interface()
			if _, ok = l.joins[c]; ok {
				// let's keep a list of the foreign keys needed for joins
				fkeys[c] = parms[i]
			}
		} else if lst, ok = l.joins[c]; ok && lst.targetName != "" {
			parms[i] = fld.Addr().Interface()
			pkIndexPtr = parms[i]
			target = getField(row, lst.targetIndex)

//			Goose.Query.Logf(0, "target (%T): %#v", target, target)
//			Goose.Query.Logf(0, "target (%T): %#v", target.Type().Elem(), target.Type().Elem())
//			Goose.Query.Logf(0, "target (%T): %#v", target.Type().Elem().Elem(), target.Type().Elem().Elem())

//			tName = d.tableType[target.Type().Elem().Elem().Name()]
//			if _, ok = d.tables[tName].xrefs[tabName]; !ok {
			if _, ok = d.tables[lst.table].xrefs[tabName]; !ok {
				target.Set(reflect.New(reflect.SliceOf(target.Type().Elem())).Elem())
				// let's keep a list of pointers to slices needed to store the rows from foreign tables
				frows[c] = target.Addr()
			}
		} else {
			parms[i] = fld.Addr().Interface()
//			Goose.Query.Logf(1, "Col c: %d, pkIndex: %d", c, d.tables[tabName].pkIndex)
			if c == d.tables[tabName].pkIndex {
				pkIndexPtr = parms[i]
			}
		}
	}

	// Scan the current table (but not the joined ones...)
	err = s.Scan(parms...)
	if err != nil {
		Goose.Query.Logf(1, "Error scanning on list of table %s: %s", tabName, err)
		return err
	}

	// Now we scan the joined tables

	// First the pointers (just 1 row)
	for c, fkey = range fkeys {
		fld = getField(row, c)
		if *(fkey.(*PK)) == 0 {
			fld.Set(reflect.Zero(fld.Type()))
			continue
		}
		lst = l.joins[c]
		if fldName, ok = typ.Field(c).Tag.Lookup("field"); ok && len(fldName)>0 {
			opt = strings.Split(fldName, ",")
			fldName = opt[0]
		} else {
			fldName = typ.Field(c).Name
		}

//		related = reflect.New(reflect.MakeSlice(reflect.SliceOf(fld.Type().Elem()),0,0).Type())
		related = reflect.New(reflect.SliceOf(fld.Type()))
		related.Elem().Set(reflect.Append(related.Elem(), fld))
		by[fldName] = *(fkey.(*PK))
//		Goose.Query.Logf(1, "related.Interface(): %#v", related.Interface())
//		Goose.Query.Logf(1, "lst.rule: %#v", lst.rule)
//		Goose.Query.Logf(1, "by=%#v", by)
		err = d.Find(At{
			Table: related.Interface(),
			With: lst.rule,
			By: by,
		})
		if err != nil {
			return err
		}

//		Goose.Query.Logf(1, ">>>related.Interface(): %#v", related.Interface())
		if related.Elem().Len() > 0 {
			if !fld.IsValid() || fld.IsNil() || fld.IsZero() {
				fld.Set(reflect.New(fld.Type().Elem()))
			}

			l, err = d.getList(lst.table, lst.rule)
			if err != nil {
				return err
			}

			relatedRow = related.Elem().Index(0)
			for i, c = range l.cols {
				fld.Elem().Field(c).Set(relatedRow.Elem().Field(c))
			}

//			Goose.Query.Logf(1, "related.Elem().Index(0): %#v", related.Elem().Index(0).Interface())
//			Goose.Query.Logf(1, "fld: %#v", fld.Elem().Interface())
		}
	}

	// Then the Slices (many rows)
	pkIndex = *(pkIndexPtr.(*PK));
	for c, fld = range frows {
		Goose.Query.Logf(0, "************************************* c=%s, l.joins=%#v", c, l.joins)
		lst = l.joins[c]

/*
		if fldName, ok = typ.Field(lst.targetIndex).Tag.Lookup("field"); ok && len(fldName)>0 {
			opt = strings.Split(fldName, ",")
			fldName = opt[0]
		} else {
			fldName = typ.Field(lst.targetIndex).Name
		}
*/
		fkTabName, _, _, err = d.getMultiRefs(fld.Interface())
		if err != nil {
			Goose.Query.Logf(1, "Parameter type error: %s", ErrNoTablesFound)
			return ErrNoTablesFound
		}

		Goose.Query.Logf(1, "************************************* d.tables[fkTabName]=%#v", d.tables[fkTabName])
		Goose.Query.Logf(1, "************************************* d.list[%s]=%#v", tabName, d.list[tabName])
		Goose.Query.Logf(1, "************************************* d.list[%s]=%#v", fkTabName, d.list[fkTabName])

		if fkTable, ok = d.tables[fkTabName]; !ok {
			Goose.Query.Logf(1, "Parameter type error: %s", ErrNoTablesFound)
			return ErrNoTablesFound
		}

		for _, fkField = range fkTable.fields {
			if fkField.fk == tabName {
				break
			}
		}

		if fkField.fk != tabName {
			Goose.Query.Logf(1, "Parameter type error: %s", ErrNoTablesFound)
			return ErrNoTablesFound
		}
		
		Goose.Query.Logf(1, "************************************* by[fkField.name](%s) = pkIndex=%#v", by[fkField.name], pkIndex)
		by[fkField.name] = pkIndex


		Goose.Query.Logf(1, "related.Interface(): %#v", fld.Interface())
		Goose.Query.Logf(1, "lst.rule: %#v", lst.rule)
		Goose.Query.Logf(1, "l.joins=%#v", l.joins)
		Goose.Query.Logf(1, "c=%d", c)
		Goose.Query.Logf(1, "by=%#v", by)


		err = d.Find(At{
			Table: fld.Interface(),
			With: lst.rule,
			By: by,
		})
		if err != nil {
			return err
		}


		for n:=0; n<fld.Elem().Len(); n++ {
			Goose.Query.Logf(1, "###################################################### resultby=%#v", fld.Elem().Index(n).Elem().Interface())
		}


	}

//	Goose.Query.Logf(1, "$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$ resultby=%#v", row.Interface())

	return nil
}

func (d *Dna) Find(at At) error {
	var tabName string
	var refRow reflect.Value
	var err error
	var isChan bool
	var l *list

//Goose.Query.Logf(0,"A")

	tabName, refRow, isChan, err = d.getMultiRefs(at.Table)
//Goose.Query.Logf(0,"B")
	if err != nil {
		Goose.Query.Logf(1, "Parameter type error: %s", ErrNoTablesFound)
		return ErrNoTablesFound
	}

Goose.Query.Logf(0,"<><><><><><><><><><><><><><><><> tabName:%s, at:%#v", tabName, at)

	if len(at.With) == 0 || at.With == "0" {
		l, err = d.getList(tabName, "0")
	} else {
		l, err = d.getList(tabName, at.With)
	}
	if err != nil {
		return err
	}

//Goose.Query.Logf(0,"D")
	
	err = d.BindParameter(tabName, at, l.stmt)
	if err != nil {
		Goose.Query.Logf(1, "Bind parameter error: %s", err)
		return err
	}

//Goose.Query.Logf(0,"E")

	if isChan {
		go func() {
			err = l.stmt.Select(func(s *sqlite.Stmt) error {
				var row reflect.Value
				var err error

				row = reflect.New(refRow.Type().Elem().Elem())

				err = d.nextRow(tabName, l, row, s, at.By, row.Elem().Type(), func(r reflect.Value, n int) reflect.Value {
					return r.Elem().Field(n)
				})

				if err != nil {
					return err
				}

				refRow.Send(row)
				
				return nil
			})
			refRow.Close()
			if err != nil {
				Goose.Query.Logf(1, "List error on %s: %s", tabName, err)
				return
			}
		}()
	} else {
//Goose.Query.Logf(0,"F")
		refRow.Set(reflect.MakeSlice(refRow.Type(), 0, 16))
//Goose.Query.Logf(0,"G")
		err = l.stmt.Select(func(s *sqlite.Stmt) error {
			var row reflect.Value
			var err error

			row = reflect.New(refRow.Type().Elem().Elem()).Elem() //?
			Goose.Query.Logf(0, "!!!!!!!!!!!!!!!!!!!!!!! l: %#v", l)
//			Goose.Query.Logf(1, "row: %#v", row)
			err = d.nextRow(tabName, l, row, s, at.By, row.Type(), func(r reflect.Value, n int) reflect.Value {
//				Goose.Query.Logf(1, "r: %#v", r)
				return r.Field(n)
			})

			if err != nil {
				return err
			}

			refRow.Set(reflect.Append(refRow, row.Addr()))

			return nil
		})
		if err != nil {
			Goose.Query.Logf(1, "List error on %s: %s", tabName, err)
			return err
		}
	}
//Goose.Query.Logf(0,"H")
	return nil
}
