package dnaoracle

import (
	"context"
	"database/sql/driver"
//   "github.com/sijms/go-ora/v2"
	"github.com/luisfurquim/dna"
)

func (drv *Driver) Delete(tabName string, at dna.At) error{
	var err error
	var stmt *Stmt
	var ok bool
	var namedArgs []driver.NamedValue

	if _, ok = drv.delete[tabName]; !ok {
		Goose.Query.Logf(1,"Error binding parameters for table %s: %s", tabName, ErrNoStmtForTable)
		return ErrNoStmtForTable
	}

	if stmt, ok = drv.delete[tabName][at.With]; !ok {
		Goose.Query.Logf(1,"Error binding parameters for table %s, rule: %s", tabName, at.With, ErrNoStmtForRule)
		return ErrNoStmtForRule
	}
	
	namedArgs, err = drv.BindParameter(tabName, at)
	if err != nil {
		Goose.Query.Logf(1,"Error binding parameters for table %s, rule %s: %s", tabName, at.With, err)
		return err
	}

	_, err = stmt.ExecContext(context.Background(), namedArgs)

	if err != nil {
		Goose.Query.Logf(1,"Error deleting from table %s, rule %s, sql: %s: %s", tabName, at.With, stmt.SQL, err)
	}
	
	return err
}
