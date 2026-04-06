package dnaoracle

import (
	"context"
	"database/sql/driver"
	"github.com/luisfurquim/dna"
)

func (drv *Driver) UpdateAt(tabName string, at dna.At, parms []driver.NamedValue) error {
	var err error
	var stmt *Stmt
	var ok bool

	if _, ok = drv.update[tabName]; !ok {
		Goose.Query.Logf(1,"Error binding parameters for table %s: %s", tabName, ErrNoStmtForTable)
		return ErrNoStmtForTable
	}

	if stmt, ok = drv.update[tabName][at.With]; !ok {
		Goose.Query.Logf(1,"Error binding parameters for table %s, rule %s: %s", tabName, at.With, ErrNoStmtForRule)
		return ErrNoStmtForRule
	}

	_, err = stmt.ExecContext(context.Background(), parms)
	if err != nil {
		Goose.Query.Logf(1,"Error executing update on table %s, rule %s: %s", tabName, at.With, err)
		return err
	}

	return nil
}
