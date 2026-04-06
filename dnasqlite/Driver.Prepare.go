package dnasqlite

import (
	"strings"
	"github.com/gwenn/gosqlite"
	"github.com/luisfurquim/dna"
)

func (drv *Driver) Prepare(stmtSpec *dna.StmtSpec) error {
	var stmt string
	var err error
	var i int
	var col dna.StmtColSpec
	var fn, alias string
	var ok bool
	var st *sqlite.Stmt
	var e string

	if stmtSpec.Clause!=dna.DeleteClause && len(stmtSpec.Columns) == 0 {
		return ErrNoColumns
	}

	switch stmtSpec.Clause {
	case dna.SelectClause:
		stmt = "SELECT "
		for i, col = range stmtSpec.Columns {
			if i>0 {
				stmt += ", "
			}
			if fn, ok = stmtSpec.ColFunc[i]; ok {
				stmt += fn + "("
			}
			if col.Column=="rowid" && stmtSpec.PkName!="" {
				stmt += "`" + col.Column + "` AS `**`"
			} else {
				stmt += "`" + col.Column + "`"
			}
			if ok {
				stmt += ")"
			}
			if alias, ok = stmtSpec.Aliases[i]; ok {
				stmt += " AS " + alias
			}
		}

		stmt += " FROM `" + stmtSpec.Table + "` "

		if len(stmtSpec.Filter) > 0 {
			e, err = expr(stmtSpec.Filter, stmtSpec.PkName)
			if err != nil {
				Goose.Init.Logf(1,"Error translating where clause %s: %s", stmtSpec.Filter, err)
				return err
			}
			Goose.Init.Logf(4,"xlate %s -> %s", stmtSpec.Filter, e)
			stmt += "WHERE " + e
		}

		//TODO: Group

		if len(stmtSpec.Sort) > 0 {
			stmt += " ORDER BY "
			for i=0; i<len(stmtSpec.Sort); i++ {
				if i>0 {
					stmt += ", "
				}
				stmt += "`" + stmtSpec.Sort[i] + "`"
				if len(stmtSpec.SortDir)>i && stmtSpec.SortDir[i] == ">" {
					stmt += " DESC"
				}
			}
		}
		
		if len(stmtSpec.Limit) > 0 {
			stmt += " LIMIT " + stmtSpec.Limit
		}

	case dna.CountClause:
		stmt = "SELECT count(rowid)  FROM `" + stmtSpec.Table + "`"
		if len(stmtSpec.Filter) > 0 {
			e, err = expr(stmtSpec.Filter, stmtSpec.PkName)
			if err != nil {
				Goose.Init.Logf(1,"Error translating where clause %s: %s", stmtSpec.Filter, err)
				return err
			}
			Goose.Init.Logf(4,"xlate %s -> %s", stmtSpec.Filter, e)
			stmt += " WHERE " + e
		}

	case dna.InsertClause:
		stmt = "INSERT INTO `" + stmtSpec.Table + "` ("

		for i, col = range stmtSpec.Columns {
			if i>0 {
				stmt += ", "
			}
			stmt += "`" + col.Column + "`"
		}

		stmt += ") VALUES ("

		for i, col = range stmtSpec.Columns {
			if i>0 {
				stmt += ", "
			}
			if fn, ok = stmtSpec.ColFunc[i]; ok {
				stmt += fn + "("
			}
			switch col.Type {
			case dna.VarColType:
				stmt += ":"
			case dna.StringColType:
				stmt += "'"
			}
			stmt += col.Value
			if col.Type==dna.StringColType {
				stmt += "'"
			}
			if ok {
				stmt += ")"
			}
		}

		stmt += ")"
	case dna.UpdateClause:
		stmt = "UPDATE `" + stmtSpec.Table + "` SET "

		for i, col = range stmtSpec.Columns {
			if i>0 {
				stmt += ", "
			}
			stmt += "`" + col.Column + "`="

			if fn, ok = stmtSpec.ColFunc[i]; ok {
				stmt += fn + "("
			}
			switch col.Type {
			case dna.VarColType:
				stmt += ":"
			case dna.StringColType:
				stmt += "'"
			}
			stmt += col.Value
			if col.Type==dna.StringColType {
				stmt += "'"
			}
			if ok {
				stmt += ")"
			}
		}

		if len(stmtSpec.Filter) > 0 {
			e, err = expr(stmtSpec.Filter, stmtSpec.PkName)
			if err != nil {
				Goose.Init.Logf(1,"Error translating where clause %s: %s", stmtSpec.Filter, err)
				return err
			}
			Goose.Init.Logf(4,"xlate %s -> %s", stmtSpec.Filter, e)
			stmt += " WHERE " + e + " "
		}

	case dna.DeleteClause:
		stmt = "DELETE FROM `" + stmtSpec.Table + "`"
		if len(stmtSpec.Filter) > 0 {
			e, err = expr(stmtSpec.Filter, stmtSpec.PkName)
			if err != nil {
				Goose.Init.Logf(1,"Error translating where clause %s: %s", stmtSpec.Filter, err)
				return err
			}
			Goose.Init.Logf(4,"xlate %s -> %s", stmtSpec.Filter, e)
			stmt += " WHERE " + e + " "
		}

	default:
		return ErrUnsupportedClause
	}

	stmt = strings.ReplaceAll(stmt, "`" + stmtSpec.PkName + "`", "rowid")
	stmt = strings.ReplaceAll(stmt, " AS `**`", " AS `" + stmtSpec.PkFieldName + "`")

	st, err = drv.db.Prepare(stmt)
	if err != nil {
		Goose.Init.Logf(1,"Err compiling stmt %s/%s: %s", stmtSpec.Table, stmtSpec.Rule, err)
//				Goose.Init.Logf(1,"SQL: %s", fmt.Sprintf("SELECT %s FROM `%s` ORDER BY rowid", colNames, tabName))
		return err
	}

	switch stmtSpec.Clause {
	case dna.SelectClause:
		if drv.find == nil {
			drv.find = map[string]map[string]*sqlite.Stmt{}
		}

		if _, ok = drv.find[stmtSpec.Table]; !ok {
			drv.find[stmtSpec.Table] = map[string]*sqlite.Stmt{}
		}

		drv.find[stmtSpec.Table][stmtSpec.Rule] = st

	case dna.CountClause:
		if drv.count == nil {
			drv.count = map[string]map[string]*sqlite.Stmt{}
		}

		if _, ok = drv.count[stmtSpec.Table]; !ok {
			drv.count[stmtSpec.Table] = map[string]*sqlite.Stmt{}
		}

		drv.count[stmtSpec.Table][stmtSpec.Rule] = st

	case dna.InsertClause:
		if drv.insert == nil {
			drv.insert = map[string]map[string]*sqlite.Stmt{}
		}

		if _, ok = drv.insert[stmtSpec.Table]; !ok {
			drv.insert[stmtSpec.Table] = map[string]*sqlite.Stmt{}
		}

		drv.insert[stmtSpec.Table][stmtSpec.Rule] = st

	case dna.UpdateClause:
		if drv.update == nil {
			drv.update = map[string]map[string]*sqlite.Stmt{}
		}

		if _, ok = drv.update[stmtSpec.Table]; !ok {
			drv.update[stmtSpec.Table] = map[string]*sqlite.Stmt{}
		}

		drv.update[stmtSpec.Table][stmtSpec.Rule] = st

	case dna.DeleteClause:
		if drv.delete == nil {
			drv.delete = map[string]map[string]*sqlite.Stmt{}
		}

		if _, ok = drv.delete[stmtSpec.Table]; !ok {
			drv.delete[stmtSpec.Table] = map[string]*sqlite.Stmt{}
		}

		drv.delete[stmtSpec.Table][stmtSpec.Rule] = st
	}

	return nil
}

