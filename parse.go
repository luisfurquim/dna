package dna

import (
	"reflect"
	"strings"
	"unicode"
)

// parseResult holds the parsed schema information for a single table.
type parseResult struct {
	TabName      string
	FldList      []FieldSpec
	Xrefs        map[string]string
	PkName       string
	PkColumnName string
	PkIndex      int
	MigrateExprs map[string]string
	RenameMap    map[string]string
	TmpList      map[string]listSpec
	TmpSaveList  map[string]listSpec
	CountSpec    map[string]string
}

// buildTableTypeMap constructs the mapping from struct type names to table names.
// This is the same logic as the first pass in New().
func buildTableTypeMap(tables []interface{}) (map[string]string, error) {
	tableType := make(map[string]string, len(tables))

	for _, tab := range tables {
		reftab := reflect.TypeOf(tab)
		if reftab.Kind() != reflect.Struct {
			Goose.Init.Logf(1, "Error on %s: %s", reftab.Name(), ErrSpecNotStruct)
			return nil, ErrSpecNotStruct
		}

		tabName := reftab.Name()
		for i := 0; i < reftab.NumField(); i++ {
			f := reftab.Field(i)
			if len(f.Name) == 0 {
				continue
			}

			isUpper := false
			for _, char := range f.Name {
				isUpper = unicode.IsUpper(char)
				break
			}
			if !isUpper {
				continue
			}

			if f.Type == TableNameType {
				if fldName, ok := f.Tag.Lookup("table"); ok && len(fldName) > 0 {
					tabName = fldName
					break
				}
				tabName = f.Name
			}
		}

		tableType[reftab.Name()] = tabName
	}

	return tableType, nil
}

// parseTableSchema extracts schema information from a single table struct.
// tableTypeMap is the mapping from struct type names to table names (from buildTableTypeMap).
func parseTableSchema(tab interface{}, tableTypeMap map[string]string) (*parseResult, error) {
	reftab := reflect.TypeOf(tab)

	result := &parseResult{
		TabName:      reftab.Name(),
		Xrefs:        make(map[string]string, 8),
		MigrateExprs: map[string]string{},
		RenameMap:    map[string]string{},
		TmpList:      map[string]listSpec{},
		TmpSaveList:  map[string]listSpec{},
		CountSpec:    map[string]string{},
		PkIndex:      -1,
	}

	fldList := make([]FieldSpec, 0, reftab.NumField())

	for i := 0; i < reftab.NumField(); i++ {
		f := reftab.Field(i)
		if len(f.Name) == 0 {
			continue
		}

		if ignore, ok := f.Tag.Lookup("dna"); ok && ignore == "-" {
			continue
		}

		isUpper := false
		for _, char := range f.Name {
			isUpper = unicode.IsUpper(char)
			break
		}
		if !isUpper {
			continue
		}

		if f.Type == TableNameType {
			if fldName, ok := f.Tag.Lookup("table"); ok && len(fldName) > 0 {
				result.TabName = fldName
			} else {
				result.TabName = f.Name
			}

		} else if f.Type == CountType {
			if cspec, ok := f.Tag.Lookup("by"); ok && len(cspec) > 0 {
				result.CountSpec[f.Name] = cspec
			}

		} else if f.Type == FindType {
			if colNames, ok := f.Tag.Lookup("cols"); ok && len(colNames) > 0 {
				sortSpec, _ := f.Tag.Lookup("sort")
				filterSpec, _ := f.Tag.Lookup("by")
				limitSpec, _ := f.Tag.Lookup("limit")

				lSpec := listSpec{
					cols:   strings.Split(colNames, ","),
					filter: filterSpec,
					limit:  limitSpec,
				}
				if len(sortSpec) > 0 {
					lSpec.sort = strings.Split(sortSpec, ",")
				}
				result.TmpList[f.Name] = lSpec
			}

		} else if f.Type == SaveType {
			if colNames, ok := f.Tag.Lookup("cols"); ok && len(colNames) > 0 {
				filterSpec, _ := f.Tag.Lookup("by")
				result.TmpSaveList[f.Name] = listSpec{
					cols:   strings.Split(colNames, ","),
					filter: filterSpec,
				}
			}

		} else {
			var fld FieldSpec

			if f.Type == PKType {
				result.PkName = f.Name
				result.PkIndex = i
				fld.PK = true
			}

			fld.JoinList = false
			if fldName, ok := f.Tag.Lookup("field"); ok && len(fldName) > 0 {
				opt := strings.Split(fldName, ",")
				fld.Name = opt[0]
			} else {
				fld.Name = f.Name
			}

			if fld.PK {
				result.PkColumnName = fld.Name
			}

			fld.Type = f.Type
			if fldPrec, ok := f.Tag.Lookup("prec"); ok && len(fldPrec) > 0 {
				fld.Prec = strings.Split(fldPrec, ",")
			}

			if migrateExpr, ok := f.Tag.Lookup("migrate"); ok && len(migrateExpr) > 0 {
				result.MigrateExprs[fld.Name] = migrateExpr
			}
			if migrateFrom, ok := f.Tag.Lookup("migrate_from"); ok && len(migrateFrom) > 0 {
				result.RenameMap[fld.Name] = migrateFrom
			}

			fld.Fk = ""
			if f.Type.Kind() == reflect.Pointer {
				if fk, ok := tableTypeMap[f.Type.Elem().Name()]; ok {
					fld.Fk = fk
					fld.Name = result.PkColumnName + "_" + fld.Name
					fld.Type = f.Type.Elem()
				}
			} else if f.Type.Kind() == reflect.Slice && f.Type.Elem().Kind() != reflect.Uint8 {
				if xref, ok := tableTypeMap[f.Type.Elem().Elem().Name()]; ok {
					result.Xrefs[xref] = fld.Name
				}
				fld.JoinList = true
			}

			fld.Index = i
			if !fld.JoinList {
				fldList = append(fldList, fld)
			}
		}
	}

	result.FldList = fldList
	return result, nil
}
