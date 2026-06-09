package dnaoci

import (
	"strings"

	"github.com/luisfurquim/dna"
)

func (drv *Driver) Prepare(stmtSpec *dna.StmtSpec) error {
	var sqlStr string
	var err error
	var i int
	var col dna.StmtColSpec
	var fn, alias string
	var ok bool
	var e string
	var offset, count string
	var lim []string
	var qt string
	var parmIdx int

	if drv.use_quotes {
		qt = `"`
	}

	if stmtSpec.Clause != dna.DeleteClause && stmtSpec.Clause != dna.CountClause && len(stmtSpec.Columns) == 0 {
		return ErrNoColumns
	}

	// parmIdx tracks positional parameter numbering for godror (:1, :2, ...)
	parmIdx = 0

	switch stmtSpec.Clause {
	case dna.SelectClause:
		sqlStr = "SELECT "
		for i, col = range stmtSpec.Columns {
			if i > 0 {
				sqlStr += ", "
			}
			if fn, ok = stmtSpec.ColFunc[i]; ok {
				sqlStr += fn + "("
			}
			sqlStr += qt + col.Column + qt
			if ok {
				sqlStr += ")"
			}
			if alias, ok = stmtSpec.Aliases[i]; ok {
				sqlStr += " AS " + alias
			}
		}

		sqlStr += ` FROM ` + qt + stmtSpec.Table + qt + ` `

		if len(stmtSpec.Filter) > 0 {
			e, err = expr(stmtSpec.Filter)
			if err != nil {
				Goose.Init.Logf(1, "Error translating where clause %s: %s", stmtSpec.Filter, err)
				return err
			}
			sqlStr += "WHERE " + e
		}

		if len(stmtSpec.Sort) > 0 {
			sqlStr += " ORDER BY "
			for i = 0; i < len(stmtSpec.Sort); i++ {
				if i > 0 {
					sqlStr += ", "
				}
				sqlStr += qt + stmtSpec.Sort[i] + qt
				if len(stmtSpec.SortDir) > i && stmtSpec.SortDir[i] == ">" {
					sqlStr += " DESC"
				}
			}
		}

		if len(stmtSpec.Limit) > 0 {
			lim = strings.Split(stmtSpec.Limit, ":")
			if len(lim) > 1 {
				e, err = expr(lim[0])
				if err != nil {
					return err
				}
				offset = e

				e, err = expr(lim[1])
				if err != nil {
					return err
				}
				count = e
			} else {
				offset = "0"
				e, err = expr(lim[0])
				if err != nil {
					return err
				}
				count = e
			}
			sqlStr += " OFFSET " + offset + " ROWS FETCH NEXT " + count + " ROWS ONLY"
		}

	case dna.InsertClause:
		sqlStr = `INSERT INTO ` + qt + stmtSpec.Table + qt + ` (`

		for i, col = range stmtSpec.Columns {
			if i > 0 {
				sqlStr += ", "
			}
			sqlStr += qt + col.Column + qt
		}

		sqlStr += ") VALUES ("

		for i, col = range stmtSpec.Columns {
			if i > 0 {
				sqlStr += ", "
			}
			if fn, ok = stmtSpec.ColFunc[i]; ok {
				sqlStr += fn + "("
			}
			switch col.Type {
			case dna.VarColType:
				parmIdx++
				sqlStr += ":" + col.Value
			case dna.StringColType:
				sqlStr += "'" + col.Value + "'"
			default:
				parmIdx++
				sqlStr += ":" + col.Value
			}
			if ok {
				sqlStr += ")"
			}
		}

		sqlStr += `) RETURNING ` + qt + stmtSpec.PkName + qt + ` INTO :DNA_LAST_INSERTED`
		_ = parmIdx

	case dna.UpdateClause:
		sqlStr = `UPDATE ` + qt + stmtSpec.Table + qt + ` SET `

		for i, col = range stmtSpec.Columns {
			if i > 0 {
				sqlStr += ", "
			}
			sqlStr += qt + col.Column + qt + `=`

			if fn, ok = stmtSpec.ColFunc[i]; ok {
				sqlStr += fn + "("
			}
			switch col.Type {
			case dna.VarColType:
				sqlStr += ":" + col.Value
			case dna.StringColType:
				sqlStr += "'" + col.Value + "'"
			default:
				sqlStr += ":" + col.Value
			}
			if ok {
				sqlStr += ")"
			}
		}

		if len(stmtSpec.Filter) > 0 {
			e, err = expr(stmtSpec.Filter)
			if err != nil {
				Goose.Init.Logf(1, "Error translating where clause %s: %s", stmtSpec.Filter, err)
				return err
			}
			sqlStr += " WHERE " + e + " "
		}

	case dna.DeleteClause:
		sqlStr = `DELETE FROM ` + qt + stmtSpec.Table + qt
		if len(stmtSpec.Filter) > 0 {
			e, err = expr(stmtSpec.Filter)
			if err != nil {
				Goose.Init.Logf(1, "Error translating where clause %s: %s", stmtSpec.Filter, err)
				return err
			}
			sqlStr += " WHERE " + e + " "
		}

	case dna.CountClause:
		sqlStr = `SELECT count(rowid) FROM ` + qt + stmtSpec.Table + qt
		if len(stmtSpec.Filter) > 0 {
			e, err = expr(stmtSpec.Filter)
			if err != nil {
				Goose.Init.Logf(1, "Error translating where clause %s: %s", stmtSpec.Filter, err)
				return err
			}
			sqlStr += " WHERE " + e
		}

	default:
		return ErrUnsupportedClause
	}

	Goose.Init.Logf(4, "SQL: %s", sqlStr)

	st, err := drv.db.Prepare(sqlStr)
	if err != nil {
		Goose.Init.Logf(1, "Err compiling stmt %s/%s: %s", stmtSpec.Table, stmtSpec.Rule, err)
		return err
	}

	entry := &stmtEntry{
		stmt: st,
		sql:  sqlStr,
	}

	switch stmtSpec.Clause {
	case dna.SelectClause:
		if drv.find == nil {
			drv.find = map[string]map[string]*stmtEntry{}
		}
		if _, ok = drv.find[stmtSpec.Table]; !ok {
			drv.find[stmtSpec.Table] = map[string]*stmtEntry{}
		}
		drv.find[stmtSpec.Table][stmtSpec.Rule] = entry

	case dna.InsertClause:
		if drv.insert == nil {
			drv.insert = map[string]map[string]*stmtEntry{}
		}
		if _, ok = drv.insert[stmtSpec.Table]; !ok {
			drv.insert[stmtSpec.Table] = map[string]*stmtEntry{}
		}
		drv.insert[stmtSpec.Table][stmtSpec.Rule] = entry

	case dna.UpdateClause:
		if drv.update == nil {
			drv.update = map[string]map[string]*stmtEntry{}
		}
		if _, ok = drv.update[stmtSpec.Table]; !ok {
			drv.update[stmtSpec.Table] = map[string]*stmtEntry{}
		}
		drv.update[stmtSpec.Table][stmtSpec.Rule] = entry

	case dna.DeleteClause:
		if drv.delete == nil {
			drv.delete = map[string]map[string]*stmtEntry{}
		}
		if _, ok = drv.delete[stmtSpec.Table]; !ok {
			drv.delete[stmtSpec.Table] = map[string]*stmtEntry{}
		}
		drv.delete[stmtSpec.Table][stmtSpec.Rule] = entry

	case dna.CountClause:
		if drv.count == nil {
			drv.count = map[string]map[string]*stmtEntry{}
		}
		if _, ok = drv.count[stmtSpec.Table]; !ok {
			drv.count[stmtSpec.Table] = map[string]*stmtEntry{}
		}
		drv.count[stmtSpec.Table][stmtSpec.Rule] = entry
	}

	return nil
}
