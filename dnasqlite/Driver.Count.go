package dnasqlite

import (
   "github.com/gwenn/gosqlite"
	"github.com/luisfurquim/dna"
)

func (drv *Driver) Count(tabName string, at dna.At) (int64, error) {
	var err error
	var count int64
	var stmt *sqlite.Stmt
	var ok bool

	if _, ok = drv.count[tabName]; !ok {
		Goose.Query.Logf(1,"Error binding parameters for table %s: %s", tabName, ErrNoStmtForTable)
		return 0, ErrNoStmtForTable
	}

	if stmt, ok = drv.count[tabName][at.With]; !ok {
		Goose.Query.Logf(1,"Error binding parameters for table %s, rule: %s", tabName, at.With, ErrNoStmtForRule)
		return 0, ErrNoStmtForRule
	}
	
	err = drv.BindParameter(tabName, at, stmt)
	if err != nil {
		Goose.Query.Logf(1,"Error binding parameters for table %s, rule %s: %s", tabName, at.With, err)
		return 0, err
	}

	Goose.Query.Logf(1,"SQL: %s", stmt.SQL())
	Goose.Query.Logf(1,"Parms: %#v", at.By)

	_, err = stmt.SelectOneRow(&count)
	if err != nil {
		Goose.Query.Logf(1, "Count error on %s: %s", tabName, err)
		return 0, err
	}

	return count, nil
}
