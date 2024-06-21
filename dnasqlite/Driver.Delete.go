package dnasqlite

import (
   "github.com/gwenn/gosqlite"
	"github.com/luisfurquim/dna"
)

func (drv *Driver) Delete(tabName string, at dna.At) error{
	var err error
	var stmt *sqlite.Stmt
	var ok bool

	if _, ok = drv.delete[tabName]; !ok {
		Goose.Query.Logf(1,"Error binding parameters for table %s: %s", tabName, ErrNoStmtForTable)
		return ErrNoStmtForTable
	}

	if stmt, ok = drv.delete[tabName][at.With]; !ok {
		Goose.Query.Logf(1,"Error binding parameters for table %s, rule: %s", tabName, at.With, ErrNoStmtForRule)
		return ErrNoStmtForRule
	}
	
	err = drv.BindParameter(tabName, at, stmt)
	if err != nil {
		Goose.Query.Logf(1,"Error binding parameters for table %s, rule %s: %s", tabName, at.With, err)
		return err
	}

	return stmt.Exec()
}
