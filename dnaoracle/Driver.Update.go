package dnaoracle

import (
	"context"
	"database/sql/driver"
//   "github.com/sijms/go-ora/v2"
)


func (drv *Driver) Update(tabName string, pk driver.NamedValue, parms []driver.NamedValue) error {
	var err error
	var stmt *Stmt
	var ok bool

	if _, ok = drv.update[tabName]; !ok {
		Goose.Query.Logf(1,"Error binding parameters for table %s: %s", tabName, ErrNoStmtForTable)
		return ErrNoStmtForTable
	}

	if stmt, ok = drv.update[tabName]["id"]; !ok {
		Goose.Query.Logf(1,"Error binding parameters for table %s: %s", tabName, ErrNoStmtForRule)
		return ErrNoStmtForRule
	}

	parms = append(parms, pk)

	_, err = stmt.ExecContext(context.Background(), parms)
	if err != nil {
		Goose.Query.Logf(1,"Error executing insert on table %s: %s", tabName, err)
		return err
	}

	return nil
}

