package dna

import (
	"fmt"
	"strings"
	"reflect"
	"unicode"
	"github.com/gwenn/gosqlite"
)

func New(db *sqlite.Conn, schema Schema) (*Dna, error) {
	var tab interface{}
	var reftab reflect.Type
	var tabName string
	var f reflect.StructField
	var fld field
	var fldName string
	var fldList []field
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
	var pkIndex int
	var tmpList map[string]listSpec
	var rule string
	var spec listSpec
	var stmt *sqlite.Stmt
	var cols []int
	var colNames string
	var colDef string
	var sortDef string
	var limitDef string
//	var filterLen int
	var sortSpec string
	var filterSpec string
	var limitSpec string
	var parts []string
	var allJoins map[string]map[string]map[int]tabRule
	var flds map[int]tabRule
	var index int
	var fRule tabRule
	var pkAdded bool
	var target reflect.Type
	var countSpec map[string]string
	var selector, cspec string
	var ignore string

	if len(schema.Tables) == 0 {
		Goose.Init.Logf(1,"Error: %s", ErrNoTablesFound)
		return nil, ErrNoTablesFound
	}

	d.db		   = db
	d.tables   = map[string]table{}
	d.insert   = map[string]*sqlite.Stmt{}
	d.link     = map[string]*sqlite.Stmt{}
	d.updateBy = map[string]map[string]*sqlite.Stmt{}
	d.count    = map[string]map[string]*sqlite.Stmt{}
	d.list     = map[string]map[string]*list{}
	d.listBy   = map[string]map[string]*sqlite.Stmt{}
	d.exists   = map[string]map[string]*sqlite.Stmt{}
	d.delete   = map[string]map[string]*sqlite.Stmt{}

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
		fldList   = make([]field,0,reftab.NumField())
		xrefs     = make(map[string]string,8)
		tmpList   = map[string]listSpec{}
		countSpec = map[string]string{}
		pkIndex   = -1

tableLoop:
		for i=0; i<reftab.NumField(); i++ {
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

			} else if f.Type == PKType {
				fld.joinList = false
				pkName = f.Name
				pkIndex = i

			} else {
				fld.joinList = false
				if fldName, ok = f.Tag.Lookup("field"); ok && len(fldName)>0 {
					opt = strings.Split(fldName, ",")
					fld.name = opt[0]
				} else {
					fld.name = f.Name
				}

				fld.fk = ""
				if f.Type.Kind() == reflect.Pointer {
					if fk, ok = d.tableType[f.Type.Elem().Name()]; ok {
						fld.fk = fk
						fld.name = "id_" + fld.name
//					} else {
//						Goose.Init.Logf(0, "fld.name: %s => %s", fld.name, f.Type.Name())
					}
				} else if f.Type.Kind() == reflect.Slice && f.Type.Elem().Kind() != reflect.Uint8 {
//					Goose.Init.Logf(0, "************fld.name: %s => %s", fld.name, f.Type.Name())
//					Goose.Init.Logf(0, "************fld.name: %s => %s", fld.name, f.Type.Elem().Name())
//					Goose.Init.Logf(0, "************fld.name: %s => %s", fld.name, f.Type.Elem().Elem().Name())
					if xref, ok = d.tableType[f.Type.Elem().Elem().Name()]; ok {
						xrefs[xref] = fld.name
//					} else {
//						Goose.Init.Logf(0, "*fld.name: %s => %s", fld.name, f.Type.Name())
					}
//					Goose.Init.Logf(0, "............xrefs: %#v", xrefs)

					fld.joinList = true
//				} else {
//					Goose.Init.Logf(0, "fld.name: %s => %s", fld.name, f.Type.Name())
				}

				fld.index = i
				if !fld.joinList {
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

			colNames, cols = fieldJoin(fldList)
//			colNames = "`" + strings.Replace(colNames,",","`,`",-1) + "`"

			err = db.Exec(fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s` (%s)", tabName, colNames))
			if err != nil {
				Goose.Init.Logf(1,"Error creating %s table: %s", tabName, err)
//				Goose.Init.Logf(1,"SQL: %s", fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s` (%s)", tabName, colNames))
				return nil, err
			}

			colNames = "rowid," + colNames
			cols = append([]int{pkIndex}, cols...)

			stmt, err = db.Prepare(fmt.Sprintf("SELECT %s FROM `%s` ORDER BY rowid", colNames, tabName))
			if err != nil {
				Goose.Init.Logf(1,"Err compiling list * from %s: %s", tabName, err)
//				Goose.Init.Logf(1,"SQL: %s", fmt.Sprintf("SELECT %s FROM `%s` ORDER BY rowid", colNames, tabName))
				return nil, err
			}

			d.list[tabName]["*"] = &list{
//				tabName: tabName,
				cols: cols,
				stmt: stmt,
			}

			if len(tmpList) > 0 {
				for rule, spec = range tmpList {
					cols = make([]int, len(spec.cols))
					colDef = ""
					pkAdded = false
					for j=0; j<len(spec.cols); j++ {
						fRule = tabRule{}
						parts = strings.Split(spec.cols[j], ":")

						if parts[0] == pkName || parts[0] == "rowid" {
							cols[j] = pkIndex
							if len(colDef) > 0 {
								colDef += ","
							}
							colDef += "rowid"
							k = pkIndex
							pkAdded = true

						} else {
							k, ok = fieldByName(parts[0], fldList)
							if ok {
								cols[j] = k
								if len(colDef) > 0 {
									colDef += ","
								}
								colDef += parts[0]
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
								if len(colDef) > 0 {
									colDef += ","
								}
								colDef += "rowid"

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

					sortDef=""
					for j=0; j<len(spec.sort); j++ {
						if len(sortDef) > 0 {
							sortDef += ","
						}
						if spec.sort[j][0] == '>' {
							sortDef += spec.sort[j][1:] + " DESC"
						} else {
							sortDef += spec.sort[j]
						}
					}
					if len(sortDef) > 0 {
						sortDef = " ORDER BY " + sortDef
					}

					if len(spec.filter) > 0 {
						spec.filter = " WHERE " + spec.filter
					}

					if len(spec.limit) > 0 {
						limitDef = " LIMIT " + spec.limit
					} else {
						limitDef = ""
					}

					if !pkAdded {
						cols = append(cols, pkIndex)
						colDef += ",rowid"
					}

					stmt, err = db.Prepare(fmt.Sprintf("SELECT %s FROM `%s`%s%s%s", colDef, tabName, spec.filter, sortDef, limitDef))
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
						stmt: stmt,
					}
				}
			}

			stmt, err = db.Prepare(fmt.Sprintf("SELECT rowid FROM `%s` ORDER BY rowid",  tabName))
			if err != nil {
				Goose.Init.Logf(1,"Err compiling list 0 from %s: %s", tabName, err)
				return nil, err
			}

			d.list[tabName]["0"] = &list{
//					tabName: tabName,
				cols: []int{pkIndex},
				stmt: stmt,
			}

			colNames, cols = fieldJoin(fldList)
//			colNames = "`" + strings.Replace(colNames,",","`,`",-1) + "`"
			stmt, err = db.Prepare(fmt.Sprintf("SELECT rowid," + colNames + " FROM `%s` WHERE rowid=:rowid",  tabName))
			if err != nil {
				Goose.Init.Logf(1,"Err compiling select pk from %s: %s", tabName, err)
//				Goose.Init.Logf(1,"SQL: %s", fmt.Sprintf("SELECT rowid," + colNames + " FROM `%s` WHERE rowid=:rowid",  tabName))
				return nil, err
			}

			d.list[tabName]["id:*"] = &list{
//					tabName: tabName,
				cols: append([]int{pkIndex},cols...),
				stmt: stmt,
			}

			Goose.Init.Logf(0,`INSERT INTO ` + tabName + ` VALUES (?` + strings.Repeat(",?",fieldLen(fldList)-1) + `)`)
			d.insert[tabName], err = db.Prepare("INSERT INTO `" + tabName + "` VALUES (?" + strings.Repeat(",?",fieldLen(fldList)-1) + `)`)
			if err != nil {
				Goose.Init.Logf(1,"Err compiling insert: %s (%#v)", err, fldList)
				return nil, err
			}

			d.updateBy[tabName] = map[string]*sqlite.Stmt{}
			Goose.Init.Logf(1,"Update By ID: %s", "UPDATE `" + tabName + "` SET " + fieldJoinNameVal(fldList) + ` WHERE rowid=?`)
			d.updateBy[tabName]["id"], err = db.Prepare("UPDATE `" + tabName + "` SET " + fieldJoinNameVal(fldList) + ` WHERE rowid=?`)
			if err != nil {
				Goose.Init.Logf(1,"Err compiling updateBy: %s", err)
				return nil, err
			}

			d.count[tabName] = map[string]*sqlite.Stmt{}
			d.count[tabName]["0"], err = db.Prepare("SELECT count(rowid) FROM `" + tabName + "`")
			if err != nil {
				Goose.Init.Logf(1,"Err compiling count: %s", err)
				return nil, err
			}

			for selector, cspec = range countSpec {
				d.count[tabName][selector], err = db.Prepare("SELECT count(*) FROM `" + tabName + "` WHERE " + cspec)
//				Goose.Init.Fatalf(0,"SELECT count(rowid) FROM `" + tabName + "` WHERE " + cspec)
				if err != nil {
					Goose.Init.Logf(1,"Err compiling count: %s", err)
					return nil, err
				}
			}

/*
			d.list[tabName], err = db.Prepare(`SELECT . FROM ` + tabName)
			if err != nil {
				Goose.Init.Logf(1,"Err compiling count: %s", err)
				return nil, err
			}

			d.listBy[tabName] = map[string]*sqlite.Stmt{}
*/

			d.exists[tabName] = map[string]*sqlite.Stmt{}
			d.exists[tabName]["id"], err = db.Prepare("SELECT count(rowid) FROM `" + tabName + "` WHERE rowid=:rowid")
			if err != nil {
				Goose.Init.Logf(1,"Err compiling exists: %s", err)
				return nil, err
			}

			d.delete[tabName] = map[string]*sqlite.Stmt{}
			d.delete[tabName]["id"], err = db.Prepare("DELETE FROM `" + tabName + "` WHERE rowid=:rowid")
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

			if _, ok = d.link[refTable + "_" + tabName]; ok {
				continue
			}

			err = db.Exec(fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s_%s (id_%s, id_%s)`, tabName, refTable, tabName, refTable))
			if err != nil {
				Goose.Init.Logf(1,"Error creating %s table: %s", tabName, err)
				return nil, err
			}

			d.link[tabName + "_" + refTable], err = db.Prepare(fmt.Sprintf(`INSERT INTO %s_%s VALUES (?,?)`, tabName, refTable))
			if err != nil {
				Goose.Init.Logf(1,"Err compiling link: %s", err)
				return nil, err
			}

			d.unlink[tabName + "_" + refTable], err = db.Prepare(fmt.Sprintf(`DELETE FROM %s_%s WHERE id_%s=? AND id_%s?`, tabName, refTable, tabName, refTable))
			if err != nil {
				Goose.Init.Logf(1,"Err compiling unlink: %s", err)
				return nil, err
			}

			if len(d.listJoin[tabName]) == 0 {
				d.listJoin[tabName] = map[string]*sqlite.Stmt{}
			}
			d.listJoin[tabName][refTable], err = db.Prepare(fmt.Sprintf(`SELECT id_%s FROM %s_%s WHERE id_%s=? `, refTable, tabName, refTable, tabName))
			if err != nil {
				Goose.Init.Logf(1,"Err compiling exists: %s", err)
				return nil, err
			}

			if len(d.listJoin[refTable]) == 0 {
				d.listJoin[refTable] = map[string]*sqlite.Stmt{}
			}
			d.listJoin[refTable][tabName], err = db.Prepare(fmt.Sprintf(`SELECT id_%s FROM %s_%s WHERE id_%s=? `, tabName, tabName, refTable, refTable))
			if err != nil {
				Goose.Init.Logf(1,"Err compiling exists: %s", err)
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

