package dna

import (
	"strings"
	"reflect"
	"unicode"
)

func New(driver Driver, schema Schema) (*Dna, error) {
	var tab interface{}
	var reftab reflect.Type
	var tabName string
	var f reflect.StructField
	var fld FieldSpec
	var fldName string
	var fldPrec string
	var fldList []FieldSpec
	var ok bool
	var char rune
	var i int
	var j int
	var k int
	var d Dna
	var fk string
	var xref string
	var xrefs map[string]string
	var refTable string
	var err error
	var opt []string
	var pkName string
	var pkColumnName
	var pkIndex int
	var tmpList map[string]listSpec
	var rule string
	var spec listSpec
	var cols []int
	var colNames string
	var sortSpec string
	var filterSpec string
	var limitSpec string
	var parts []string
	var allJoins map[string]map[string]map[int]tabRule
	var flds map[int]tabRule
	var index int
	var fRule tabRule
//	var pkAdded bool
	var target reflect.Type
	var countSpec map[string]string
	var selector, cspec string
	var ignore string
	var stmtSpec *StmtSpec

	if len(schema.Tables) == 0 {
		Goose.Init.Logf(1,"Error: %s", ErrNoTablesFound)
		return nil, ErrNoTablesFound
	}

	d.driver	  = driver
	d.tables   = map[string]table{}
	d.list     = map[string]map[string]*list{}

	allJoins    = map[string]map[string]map[int]tabRule{}

	d.tableType = make(map[string]string, len(schema.Tables))
	for _, tab = range schema.Tables {
		reftab = reflect.TypeOf(tab)
		if reftab.Kind() != reflect.Struct {
			Goose.Init.Logf(1,"Error on %s: %s", reftab.Name(), ErrSpecNotStruct)
			return nil, ErrSpecNotStruct
		}

		tabName = reftab.Name()
tableLoop1:
		for i=0; i<reftab.NumField(); i++ {
			f = reftab.Field(i)
			if len(f.Name)==0 {
				continue
			}

			for _, char = range f.Name {
				if !unicode.IsUpper(char) {
					continue tableLoop1
				}
				break
			}

			if f.Type == TableNameType {
				if fldName, ok = f.Tag.Lookup("table"); ok && len(fldName)>0 {
					tabName = fldName
					break
				}
				tabName = f.Name
			}
		}

		d.tableType[reftab.Name()] = tabName
		allJoins[tabName] = map[string]map[int]tabRule{}
	}

//	Goose.Init.Logf(0,"%#v",d.tableType)

	for _, tab = range schema.Tables {
		reftab    = reflect.TypeOf(tab)
		tabName   = reftab.Name()
		fldList   = make([]FieldSpec,0,reftab.NumField())
		xrefs     = make(map[string]string,8)
		tmpList   = map[string]listSpec{}
		countSpec = map[string]string{}
		pkIndex   = -1

tableLoop:
		for i=0; i<reftab.NumField(); i++ {
			fld = FieldSpec{}
			f = reftab.Field(i)
			if len(f.Name)==0 {
				continue
			}

			if ignore, ok = f.Tag.Lookup("dna"); ok && ignore=="-" {
				continue
			}

			for _, char = range f.Name {
				if !unicode.IsUpper(char) {
					continue tableLoop
				}
				break
			}

			if f.Type == TableNameType {
				if fldName, ok = f.Tag.Lookup("table"); ok && len(fldName)>0 {
					tabName = fldName
				} else {
					tabName = f.Name
				}

			} else if f.Type == CountType {
				if cspec, ok = f.Tag.Lookup("by"); ok && len(cspec)>0 {
					countSpec[f.Name] = cspec
				}

			} else if f.Type == FindType {
				if colNames, ok = f.Tag.Lookup("cols"); ok && len(colNames)>0 {
					sortSpec, _ = f.Tag.Lookup("sort")
					filterSpec, _ = f.Tag.Lookup("by")
					limitSpec, _ = f.Tag.Lookup("limit")

					lSpec := listSpec{
						cols: strings.Split(colNames, ","),
						filter: filterSpec,
						limit: limitSpec,
					}
					if len(sortSpec) > 0 {
						lSpec.sort = strings.Split(sortSpec, ",")
					}

					tmpList[f.Name] = lSpec
				}

			} else {
				if f.Type == PKType {
					pkName = f.Name
					pkIndex = i
					fld.PK = true
				}

				fld.JoinList = false
				if fldName, ok = f.Tag.Lookup("field"); ok && len(fldName)>0 {
					opt = strings.Split(fldName, ",")
					fld.Name = opt[0]
				} else {
					fld.Name = f.Name
				}

				if fld.PK {
					pkColumnName = fld.Name
				}

				fld.Type = f.Type
				if fldPrec, ok = f.Tag.Lookup("prec"); ok && len(fldPrec)>0 {
					fld.Prec = strings.Split(fldPrec, ",")
				}

				fld.Fk = ""
				if f.Type.Kind() == reflect.Pointer {
					if fk, ok = d.tableType[f.Type.Elem().Name()]; ok {
						fld.Fk = fk
						fld.Name = "id_" + fld.Name
						fld.Type = f.Type.Elem()
//					} else {
//						Goose.Init.Logf(0, "fld.name: %s => %s", fld.name, f.Type.Name())
					}
				} else if f.Type.Kind() == reflect.Slice && f.Type.Elem().Kind() != reflect.Uint8 {
//					Goose.Init.Logf(0, "************fld.name: %s => %s", fld.name, f.Type.Name())
//					Goose.Init.Logf(0, "************fld.name: %s => %s", fld.name, f.Type.Elem().Name())
//					Goose.Init.Logf(0, "************fld.name: %s => %s", fld.name, f.Type.Elem().Elem().Name())
					if xref, ok = d.tableType[f.Type.Elem().Elem().Name()]; ok {
						xrefs[xref] = fld.Name
//					} else {
//						Goose.Init.Logf(0, "*fld.name: %s => %s", fld.name, f.Type.Name())
					}
//					Goose.Init.Logf(0, "............xrefs: %#v", xrefs)

					fld.JoinList = true
//				} else {
//					Goose.Init.Logf(0, "fld.name: %s => %s", fld.name, f.Type.Name())
				}

				fld.Index = i
				if !fld.JoinList {
					fldList = append(fldList, fld)
				}
			}
		}

		if len(fldList) > 0 {
			if pkIndex < 0 {
				Goose.Init.Logf(1,"Error creating %s table: %s", tabName, ErrNoPKFound)
				return nil, ErrNoPKFound
			}

			d.tables[tabName] = table{
				name: tabName,
				fields: fldList,
				xrefs: xrefs,
				pkName: pkName,
				pkIndex: pkIndex,
			}

			d.list[tabName] = map[string]*list{}

			colNames, cols = driver.ColumnSpecs(fldList, pkIndex)
			err = driver.CreateTable(tabName, fldList)
			if err != nil {
				Goose.Init.Logf(1,"Error creating %s table: %s", tabName, err)
				return nil, err
			}

			stmtSpec = &StmtSpec{
				Clause: SelectClause,
				Table: tabName,
				PkName: pkColumnName,
				Rule: "0",
			}

			if driver.PKName() != "" {
				stmtSpec.Columns = []StmtColSpec{StmtColSpec{Column: driver.PKName(), Pk: true}}
				stmtSpec.Sort = []string{driver.PKName()}
			} else {
				stmtSpec.Columns = []StmtColSpec{StmtColSpec{Column: pkName, Pk: true}}
				stmtSpec.Sort = []string{pkName}
			}

			err = driver.Prepare(stmtSpec)
			if err != nil {
				Goose.Init.Logf(1,"Err compiling list 0 from %s: %s", tabName, err)
				return nil, err
			}

			d.list[tabName]["0"] = &list{
				cols: []int{pkIndex},
			}

			stmtSpec = &StmtSpec{
				Clause:    SelectClause,
				Table:     tabName,
				PkName:    pkColumnName,
				Rule:		  "*",
				Columns:   fld2stmt(fldList),
				Sort:    []string{pkName},
			}

			err = driver.Prepare(stmtSpec)
			if err != nil {
				Goose.Init.Logf(1,"Err compiling list * from %s: %s", tabName, err)
//				Goose.Init.Logf(1,"SQL: %s", fmt.Sprintf("SELECT %s FROM `%s` ORDER BY rowid", colNames, tabName))
				return nil, err
			}

			d.list[tabName]["*"] = &list{
//				tabName: tabName,
				cols: cols,
			}

			for i=0; i<len(fldList); i++ {
				if fldList[i].PK {
					break
				}
			}

			// If there is no private key in the list and the database has a special name for the id of a row
			if i>=len(fldList) && driver.PKName() != "" {
				stmtSpec.Columns = append(stmtSpec.Columns, StmtColSpec{Column: driver.PKName(), Pk: true})
				stmtSpec.Filter = driver.PKName() + "==<-" + driver.PKName()
				stmtSpec.Rule = "id:*"
				err = driver.Prepare(stmtSpec)
				if err != nil {
					Goose.Init.Logf(1,"Err compiling select pk from %s: %s", tabName, err)
	//				Goose.Init.Logf(1,"SQL: %s", fmt.Sprintf("SELECT rowid," + colNames + " FROM `%s` WHERE rowid=:rowid",  tabName))
					return nil, err
				}

				d.list[tabName]["id:*"] = &list{
					cols: append([]int{pkIndex},cols...),
				}
			}

			if len(tmpList) > 0 {
				stmtSpec = &StmtSpec{
					Clause: SelectClause,
					Table: tabName,
					PkName: pkColumnName,
				}

				for rule, spec = range tmpList {
					cols = make([]int, len(spec.cols))
//					pkAdded = false
					stmtSpec.Columns = []StmtColSpec{}
					for j=0; j<len(spec.cols); j++ {
						fRule = tabRule{}
						parts = strings.Split(spec.cols[j], ":")

						if parts[0] == pkName || parts[0] == driver.PKName() {
							cols[j] = pkIndex
							if driver.PKName() != "" {
								stmtSpec.Columns = append(stmtSpec.Columns, StmtColSpec{Column: driver.PKName(), Pk: true})
							} else {
								stmtSpec.Columns = append(stmtSpec.Columns, StmtColSpec{Column: parts[0], Pk: true})
							}
							k = pkIndex
//							pkAdded = true

						} else {
							k, ok = fieldByName(parts[0], fldList)
							if ok {
								cols[j] = k
								stmtSpec.Columns = append(stmtSpec.Columns, StmtColSpec{Column: parts[0]})
								if len(parts) > 1 {
									fRule.table = d.tableType[reftab.Field(k).Type.Elem().Name()]
								}
							} else {

								if len(parts) == 1 {
									Goose.Init.Logf(1,"Err compiling list %s from %s: %s", rule, tabName, ErrColumnNotFound)
									Goose.Init.Logf(1,"tmpList %#v col [%s][%s]", tmpList, spec.cols[j], parts[0])
									Goose.Init.Logf(1,"fldList %#v", fldList)
									Goose.Init.Logf(1,"pkName %s", pkName)
									return nil, ErrColumnNotFound
								}

								k, ok = fieldByNameFromType(parts[0], reftab)
								if !ok {
									Goose.Init.Logf(1,"Err compiling list %s from %s: %s", rule, tabName, ErrColumnNotFound)
									Goose.Init.Logf(1,"tmpList %#v col %s", tmpList, spec.cols[j])
									Goose.Init.Logf(1,"fldList %#v", fldList)
									Goose.Init.Logf(1,"pkName %s", pkName)
									return nil, ErrColumnNotFound
								}

								target = reftab.Field(k).Type
								if target.Kind() != reflect.Slice && target.Elem().Kind() != reflect.Pointer {
									Goose.Init.Logf(1,"Err compiling list %s from %s: %s", rule, tabName, ErrColumnNotFound)
									Goose.Init.Logf(1,"tmpList %#v col %s", tmpList, spec.cols[j])
									Goose.Init.Logf(1,"fldList %#v", fldList)
									Goose.Init.Logf(1,"pkName %s %#v %d %d", pkName, target, target.Kind(), target.Elem().Kind())
									return nil, ErrColumnNotFound
								}

								cols[j] = pkIndex

/*
//								if driver.PKName() != "" {
//									stmtSpec.Columns = append(stmtSpec.Columns, StmtColSpec{Column: driver.PKName()})
//								} else {
									stmtSpec.Columns = append(stmtSpec.Columns, StmtColSpec{Column: parts[0]})
//								}
*/

								fRule.table	= d.tableType[reftab.Field(k).Type.Elem().Name()]
								fRule.targetName  = parts[0]
								fRule.targetIndex = k
							}

						}

						if len(parts) > 1 {
							if _, ok = allJoins[tabName][rule]; !ok {
								allJoins[tabName][rule] = map[int]tabRule{}
							}

							fRule.rule  = parts[1]
							allJoins[tabName][rule][cols[j]] = fRule
						}
					}

					stmtSpec.Sort    = nil
					stmtSpec.SortDir = nil

					for j=0; j<len(spec.sort); j++ {
						if spec.sort[j][0] == '>' {
							stmtSpec.Sort = append(stmtSpec.Sort, spec.sort[j][1:])
							stmtSpec.SortDir = append(stmtSpec.SortDir, ">")
						} else {
							stmtSpec.Sort = append(stmtSpec.Sort, spec.sort[j])
							stmtSpec.SortDir = append(stmtSpec.SortDir, "<")
						}
					}

					if len(spec.filter) > 0 {
						stmtSpec.Filter = spec.filter
					} else {
						stmtSpec.Filter = ""
					}

					if len(spec.limit) > 0 {
						stmtSpec.Limit = spec.limit
					} else {
						stmtSpec.Limit = ""
					}

/*
					if !pkAdded {
						cols = append(cols, pkIndex)
						if driver.PKName() != "" {
							stmtSpec.Columns = append(stmtSpec.Columns, StmtColSpec{Column: driver.PKName()})
						} else {
							stmtSpec.Columns = append(stmtSpec.Columns, StmtColSpec{Column: pkName})
						}
					}
*/

					stmtSpec.Rule = rule

					err = driver.Prepare(stmtSpec)
					if err != nil {
						Goose.Init.Logf(1,"Err compiling list %s from %s: %s", rule, tabName, err)
//						Goose.Init.Logf(1,"tmpList %#v", tmpList)
//						Goose.Init.Logf(1,"fldList %#v", fldList)
//						Goose.Init.Logf(1,"pkName %s, pkIndex: %d", pkName, pkIndex)
						return nil, err
					}

					d.list[tabName][rule] = &list{
//						tabName: tabName,
						cols: cols,
//						filterLen: filterLen,
//						stmt: stmt,
					}
				}
			}

			stmtSpec = &StmtSpec{
				Clause: InsertClause,
				Table: tabName,
				PkName: pkColumnName,
				Rule: "*",
			}

			for i=0; i<len(fldList); i++ {
				if fldList[i].PK {
					continue
				}
				stmtSpec.Columns = append(stmtSpec.Columns, StmtColSpec{
					Column: fldList[i].Name,
					Value:  fldList[i].Name,
					Type:   VarColType,
				})
			}

			err = driver.Prepare(stmtSpec)
			if err != nil {
				Goose.Init.Logf(1,"Err compiling insert: %s (%#v)", err, fldList)
				return nil, err
			}

			stmtSpec.Clause = UpdateClause
			stmtSpec.Rule = "id"

			for i=0; i<len(fldList); i++ {
				if fldList[i].PK {
					stmtSpec.Filter = fldList[i].Name + "==<-" + fldList[i].Name
					break
				}
			}

			err = driver.Prepare(stmtSpec)
			if err != nil {
				Goose.Init.Logf(1,"Err compiling updateBy: %s", err)
				return nil, err
			}

			stmtSpec = &StmtSpec{
				Clause: SelectClause,
				Table: tabName,
				PkName: pkColumnName,
				Rule: "#",
				Columns: []StmtColSpec{StmtColSpec{Column: pkName, Pk: true}},
				ColFunc: map[int]string{0:"count"},
			}

			err = driver.Prepare(stmtSpec)
			if err != nil {
				Goose.Init.Logf(1,"Err compiling count: %s", err)
				return nil, err
			}

			stmtSpec.Rule = "@!"
			stmtSpec.Filter = pkName + "==<-" + pkName

			err = driver.Prepare(stmtSpec)
			if err != nil {
				Goose.Init.Logf(1,"Err compiling exists: %s", err)
				return nil, err
			}

			for selector, cspec = range countSpec {
				stmtSpec.Rule = selector
				stmtSpec.Filter = cspec

				err = driver.Prepare(stmtSpec)
				if err != nil {
					Goose.Init.Logf(1,"Err compiling count: %s", err)
					return nil, err
				}
			}

			stmtSpec = &StmtSpec{
				Clause: DeleteClause,
				Table: tabName,
				PkName: pkColumnName,
				Rule: "id",
				Filter: pkName + "==<-" + pkName,
			}

			err = driver.Prepare(stmtSpec)
			if err != nil {
				Goose.Init.Logf(1,"Err compiling delete: %s", err)
				return nil, err
			}

		}
	}

	for tabName, _ = range d.tables {
		for refTable, _ = range d.tables[tabName].xrefs {
			if _, ok = d.tables[refTable].xrefs[tabName]; !ok {
				continue
			}

			if driver.Exists(refTable + "_" + tabName) {
				continue
			}

			err = driver.CreateTable(tabName + "_" + refTable, []FieldSpec{
				FieldSpec{
					Name: "id_" + tabName,
					Type: PKType,
				},
				FieldSpec{
					Name: "id_" + refTable,
					Type: PKType,
				},
			})
			if err != nil {
				Goose.Init.Logf(1,"Error creating %s table: %s", tabName, err)
				return nil, err
			}

			stmtSpec = &StmtSpec{
				Clause: InsertClause,
				Table: tabName + "_" + refTable,
				Rule: "*",
				Columns: []StmtColSpec{
					StmtColSpec{
						Column: "id_" + tabName,
						Value:  "id_" + tabName,
						Type:   VarColType,
					},
					StmtColSpec{
						Column: "id_" + refTable,
						Value:  "id_" + refTable,
						Type:   VarColType,
					},
				},
			}

			err = driver.Prepare(stmtSpec)
			if err != nil {
				Goose.Init.Logf(1,"Err compiling link: %s", err)
				return nil, err
			}

			stmtSpec.Clause = DeleteClause
			stmtSpec.Rule = "id"
			stmtSpec.Columns = nil
			stmtSpec.Filter = "id_" + tabName + "==<-id_" + tabName + " && id_" + refTable + "==<-id_" + refTable 

			err = driver.Prepare(stmtSpec)
			if err != nil {
				Goose.Init.Logf(1,"Err compiling unlink: %s", err)
				return nil, err
			}

			stmtSpec = &StmtSpec{
				Clause: SelectClause,
				Table: tabName + "_" + refTable,
				Rule: "join",
				Columns: []StmtColSpec{
					StmtColSpec{
						Column: "id_" + refTable,
					},
				},
				Filter: "id_" + tabName + "==<-id_" + tabName,
			}

			err = driver.Prepare(stmtSpec)
			if err != nil {
				Goose.Init.Logf(1,"Err compiling join: %s", err)
				return nil, err
			}

			stmtSpec = &StmtSpec{
				Clause: SelectClause,
				Table: refTable + "_" + tabName,
				Rule: "join",
				Columns: []StmtColSpec{
					StmtColSpec{
						Column: "id_" + tabName,
					},
				},
				Filter: "id_" + refTable + "==<-id_" + refTable,
			}


			err = driver.Prepare(stmtSpec)
			if err != nil {
				Goose.Init.Logf(1,"Err compiling join: %s", err)
				return nil, err
			}

		}

		for rule, flds = range allJoins[tabName] {
			if d.list[tabName][rule].joins == nil {
				d.list[tabName][rule].joins = map[int]tabRule{}
			}
			for index, fRule = range flds {
				d.list[tabName][rule].joins[index] = fRule
			}
		}
	}

	return &d, nil
}

