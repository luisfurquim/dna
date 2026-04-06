package dnasqlite

import (
	"fmt"
	"database/sql/driver"
	"github.com/gwenn/gosqlite"
	"github.com/luisfurquim/dna"
)

func (drv *Driver) UpdateAt(tabName string, at dna.At, parms []driver.NamedValue) error {
	var (
		err error
		stmt *sqlite.Stmt
		ok bool
		p driver.NamedValue
		index int
		s *sqlite.Stmt
		plist []any
	)

	if _, ok = drv.update[tabName]; !ok {
		Goose.Query.Logf(1,"Error binding parameters for table %s: %s", tabName, ErrNoStmtForTable)
		return ErrNoStmtForTable
	}

	if stmt, ok = drv.update[tabName][at.With]; !ok {
		Goose.Query.Logf(1,"Error binding parameters for table %s, rule %s: %s", tabName, at.With, ErrNoStmtForRule)
		return ErrNoStmtForRule
	}

	plist = make([]any, len(parms))

	for _, p = range parms {
		Goose.Query.Logf(4,"Will bind %#v", p)
		index, err = stmt.BindParameterIndex(":" + p.Name)
		if err != nil {
			if fmt.Sprintf("%s", err) == "bad parameter or other API misuse (Stmt.Bind) (bad parameter or other API misuse)" {
				s, err = drv.db.Prepare(stmt.SQL())
				if err != nil {
					Goose.Query.Logf(1,"Error binding parameter for %s, using parm %#v: %s", stmt.SQL(), p, err)
					return err
				}
				*stmt = *s
			}
		}
		plist[index-1] = p.Value
	}

	Goose.Query.Logf(4,"Bound parameter for %s, using parms %#v", stmt.SQL(), plist)
	return stmt.Exec(plist...)
}
