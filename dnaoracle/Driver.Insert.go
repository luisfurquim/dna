package dnaoracle

import (
	"context"
	"database/sql/driver"
//   "github.com/sijms/go-ora/v2"
	"github.com/luisfurquim/dna"
)

func (drv *Driver) Insert(tabName string, pk driver.NamedValue, parms []driver.NamedValue) (dna.PK, error) {
//	var res driver.Result
	var err error
	var stmt *Stmt
	var ok bool
	var id dna.PK
	var tx driver.Tx

	if _, ok = drv.insert[tabName]; !ok {
		Goose.Query.Logf(1,"Error binding parameters for table %s: %s", tabName, ErrNoStmtForTable)
		return 0, ErrNoStmtForTable
	}

	if stmt, ok = drv.insert[tabName]["*"]; !ok {
		Goose.Query.Logf(1,"Error binding parameters for table %s: %s", tabName, ErrNoStmtForRule)
		return 0, ErrNoStmtForRule
	}

	pk.Name = "DNA_LAST_INSERTED"
	pk.Value = &id

	parms = append(parms, pk)
	
	Goose.Query.Logf(5,"tabName: %s", tabName)
	Goose.Query.Logf(5,"SQL: %s", stmt.SQL)
	Goose.Query.Logf(6,"Parms: %#v", parms)

	tx, err = drv.db.Begin()

	_, err = stmt.ExecContext(context.Background(), parms)
	if err != nil {
		Goose.Query.Logf(1,"Error executing insert on table %s: %s", tabName, err)
		return 0, err
	}

	tx.Commit()

//	id, err = res.LastInsertId()
	Goose.Query.Logf(1,"res.LastInsertId() on table %s: %d", tabName, id)
	return id, err
}

	
