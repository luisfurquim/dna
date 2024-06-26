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
	var failedBefore bool
	var stmt *Stmt
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

/*
	Goose.Query.Logf(5,"tabName: %s", tabName)
	Goose.Query.Logf(5,"rule: %s", at.With)
	Goose.Query.Logf(5,"SQL: %s", stmt.SQL)
	Goose.Query.Logf(6,"by: %#v", at.By)
	Goose.Query.Logf(6,"Parms: %#v", namedArgs)
*/

	for {
		rows, err = stmt.QueryContext(context.Background(), namedArgs)
		if err == nil {
			break
		}

		// Fail, if there was already an error
		if failedBefore {
			Goose.Query.Logf(1,"Error selecting from table %s, rule %s: %s", tabName, at.With, err)
			return err
		}

		failedBefore = true

		stmt = &Stmt{
			Stmt: *go_ora.NewStmt(stmt.SQL, drv.db),
			SQL: stmt.SQL,
		}
		drv.find[tabName][at.With] = stmt
	}

	s = &Scanner{
		rows: rows,
	}

//	Goose.Query.Logf(1,"S0")
	for {
//		Goose.Query.Logf(0,"select loop",)
		err = fn(s)
//		Goose.Query.Logf(1,"S1")
		if err != nil {
//			stmt.Close()
//			Goose.Query.Logf(1,"S2")
			if err == io.EOF {
//				Goose.Query.Logf(1,"S3")
				return nil
			}
//			Goose.Query.Logf(1,"S4")
			return err
		}
//		Goose.Query.Logf(1,"S5")
	}
//	Goose.Query.Logf(1,"S6")

	// never reaches this
	return nil
}
