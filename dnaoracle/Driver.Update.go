package dnaoracle

import (
	"context"
	"database/sql/driver"
   "github.com/sijms/go-ora/v2"
)


func (drv *Driver) Update(tabName string, parms ...interface{}) error {
	var err error
	var stmt *Stmt
	var ok bool
	var i int
	var namedArgs []driver.NamedValue

	if _, ok = drv.update[tabName]; !ok {
		Goose.Query.Logf(1,"Error binding parameters for table %s: %s", tabName, ErrNoStmtForTable)
		return ErrNoStmtForTable
	}

	if stmt, ok = drv.update[tabName]["id"]; !ok {
		Goose.Query.Logf(1,"Error binding parameters for table %s: %s", tabName, ErrNoStmtForRule)
		return ErrNoStmtForRule
	}
	
	namedArgs = make([]driver.NamedValue,len(parms))
	for i, _ = range parms {
		namedArgs[i] = driver.NamedValue{
			Ordinal: i,
			Value: parms[i],
		}
	}
	
	_, err = stmt.ExecContext(context.Background(), namedArgs)
	if err != nil {
		Goose.Query.Logf(1,"Error executing insert on table %s: %s", tabName, err)
		return err
	}

	return nil
}

