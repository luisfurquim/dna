package dnasqlite

import (
   "github.com/gwenn/gosqlite"
	"github.com/luisfurquim/dna"
)

func (drv *Driver) Select(tabName string, at dna.At, fn func(dna.Scanner) error) error {
	var err error
	var stmt *sqlite.Stmt
	var ok bool

	if _, ok = drv.find[tabName]; !ok {
		Goose.Query.Logf(1,"Error binding parameters for table %s: %s", tabName, ErrNoStmtForTable)
		return ErrNoStmtForTable
	}

	if stmt, ok = drv.find[tabName][at.With]; !ok {
		Goose.Query.Logf(1,"Error binding parameters for table %s, rule: %s", tabName, at.With, ErrNoStmtForRule)
		return ErrNoStmtForRule
	}
	
	err = drv.BindParameter(tabName, at, stmt)
	if err != nil {
		Goose.Query.Logf(1,"Error binding parameters for table %s, rule %s: %s", tabName, at.With, err)
		return err
	}

	Goose.Query.Logf(1,"SQL: %s", stmt.SQL())
	Goose.Query.Logf(1,"Parms: %#v", at.By)

	err = stmt.Select(func(s *sqlite.Stmt) error {
		return fn(s)
	})
	
	return err
}

