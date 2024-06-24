package dnaoracle

import (
	"context"
	"database/sql/driver"
   "github.com/sijms/go-ora/v2"
	"github.com/luisfurquim/dna"
)

func (drv *Driver) Insert(tabName string, parms ...interface{}) (dna.PK, error) {
	var id int64
	var res driver.Result
	var err error
	var stmt *go_ora.Stmt
	var ok bool
	var i int
	var namedArgs []driver.NamedValue
	var b bool
	var parm interface{}

	if _, ok = drv.insert[tabName]; !ok {
		Goose.Query.Logf(1,"Error binding parameters for table %s: %s", tabName, ErrNoStmtForTable)
		return 0, ErrNoStmtForTable
	}

	if stmt, ok = drv.insert[tabName]["*"]; !ok {
		Goose.Query.Logf(1,"Error binding parameters for table %s: %s", tabName, ErrNoStmtForRule)
		return 0, ErrNoStmtForRule
	}

	namedArgs = make([]driver.NamedValue,len(parms))
	for i, parm = range parms {
		if b, ok = parm.(bool); ok {
			if b {
				parm = "T"
			} else {
				parm = "F"
			}
			namedArgs[i] = driver.NamedValue{
				Ordinal: i,
				Value: parm,
			}
		} else {
			namedArgs[i] = driver.NamedValue{
				Ordinal: i,
				Value: parms[i],
			}
		}
	}
	
	res, err = stmt.ExecContext(context.Background(), namedArgs)
	if err != nil {
		Goose.Query.Logf(1,"Error executing insert on table %s: %s", tabName, err)
		return 0, err
	}

	id, err = res.LastInsertId()
	Goose.Query.Logf(1,"res.LastInsertId() on table %s: %d", tabName, id)
	return dna.PK(id), err
}

	
