package dna

import (
	"reflect"
)
 
func (d *Dna) getMultiRefs(row interface{}) (string, reflect.Value, bool, error) {
	var rowType string
	var tabName string
	var reftab reflect.Type
	var refRow reflect.Value
	var ok bool
	var isChan bool

	reftab = reflect.TypeOf(row)

//	Goose.Init.Logf(0,"%#v\n%d", row, reftab.Kind())

	if reftab.Kind() == reflect.Chan {
		isChan = true
		reftab = reftab.Elem()

		if reftab.Kind() != reflect.Pointer {
			Goose.Query.Logf(1, "Parameter type error: %s", ErrNotStructPointerChan)
			return "", refRow, isChan, ErrNotStructPointerChan
		}

		reftab = reftab.Elem()
		if reftab.Kind() != reflect.Struct {
			Goose.Query.Logf(1, "Parameter type error: %s", ErrNotStructPointerChan)
			return "", refRow, isChan, ErrNotStructPointerChan
		}
	} else {
		if reftab.Kind() != reflect.Pointer {
			Goose.Query.Logf(1, "Parameter type error: %s", ErrNotStructSlicePointer)
			return "", refRow, isChan, ErrNotStructSlicePointer
		}

//Goose.Query.Logf(0,"1C")

		reftab = reftab.Elem()
		if reftab.Kind() != reflect.Slice {
			Goose.Query.Logf(1, "Parameter type error: %s", ErrNotStructSlicePointer)
			return "", refRow, isChan, ErrNotStructSlicePointer
		}

//Goose.Query.Logf(0,"2C")

		reftab = reftab.Elem()
		if reftab.Kind() != reflect.Pointer {
			Goose.Query.Logf(1, "Parameter type error: %s", ErrNotStructSlicePointer)
			return "", refRow, isChan, ErrNotStructSlicePointer
		}

//Goose.Query.Logf(0,"3C")

		reftab = reftab.Elem()
		if reftab.Kind() != reflect.Struct {
			Goose.Query.Logf(1, "Parameter type error: %s", ErrNotStructSlicePointer)
			return "", refRow, isChan, ErrNotStructSlicePointer
		}
	}

//Goose.Query.Logf(0,"4C")

	rowType = reftab.Name()
//	Goose.Query.Logf(0,"4C1 [%s]", rowType)
//	Goose.Query.Logf(0,"4C1 %#v", d)
//	Goose.Query.Logf(0,"4C1 %#v", d.tableType)
	if tabName, ok = d.tableType[rowType]; !ok {
//		Goose.Query.Logf(0,"4C2")
		Goose.Query.Logf(1, "Parameter type error: %s", ErrNoTablesFound)
		return "", refRow, isChan, ErrNoTablesFound
	}

//Goose.Query.Logf(0,"5C")

	refRow = reflect.ValueOf(row)
	if !refRow.IsValid() || refRow.IsNil() || refRow.IsZero() {
		Goose.Query.Logf(1, "Parameter error", ErrInvalid)
		return "", refRow, isChan, ErrInvalid
	}

//Goose.Query.Logf(0,"6C")

	if isChan {
		return tabName, refRow, isChan, nil
	}

//Goose.Query.Logf(0,"7C")

	return tabName, refRow.Elem(), isChan, nil
}