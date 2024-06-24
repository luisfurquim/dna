package dnaoracle

import (
	"io"
	"context"
	"database/sql/driver"
   "github.com/sijms/go-ora/v2"
	"github.com/luisfurquim/dna"
)

func (drv *Driver) Select(tabName string, at dna.At, fn func(dna.Scanner) error) error {
	var err error
	var stmt *go_ora.Stmt
	var ok bool
	var rows driver.Rows
	var namedArgs []driver.NamedValue
	var s *Scanner

	if _, ok = drv.find[tabName]; !ok {
		Goose.Query.Logf(1,"Error binding parameters for table %s: %s", tabName, ErrNoStmtForTable)
		return ErrNoStmtForTable
	}

	if stmt, ok = drv.find[tabName][at.With]; !ok {
		Goose.Query.Logf(1,"Error binding parameters for table %s, rule: %s", tabName, at.With, ErrNoStmtForRule)
		return ErrNoStmtForRule
	}
	
	namedArgs, err = drv.BindParameter(tabName, at)
	if err != nil {
		Goose.Query.Logf(1,"Error binding parameters for table %s, rule %s: %s", tabName, at.With, err)
		return err
	}

	Goose.Query.Logf(6,"by: %#v", at.By)
	Goose.Query.Logf(6,"Parms: %#v", namedArgs)

	rows, err = stmt.QueryContext(context.Background(), namedArgs)
	if err != nil {
		Goose.Query.Logf(1,"Error selecting from table %s, rule %s: %s", tabName, at.With, err)
		return err
	}

	s = &Scanner{
		rows: rows,
	}

	for {
//		Goose.Query.Logf(0,"select loop",)
		err = fn(s)
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
	}

	// never reaches this
	return nil
}