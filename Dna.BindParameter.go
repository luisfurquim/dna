package dna

import (
	"fmt"
   "github.com/gwenn/gosqlite"
)

func (d *Dna) BindParameter(tabName string, at At, 	stmt *sqlite.Stmt) error {
	var index int
	var parmName string
	var parm interface{}
	var err error
	var s *sqlite.Stmt

	for parmName, parm = range at.By {
		index, err = stmt.BindParameterIndex(":" + parmName)
//		Goose.Query.Logf(1, "will bind %s with %#v", parmName, parm)
		if err == nil {
			err = stmt.BindByIndex(index, parm)
//			Goose.Query.Logf(1, "bound %s (%d) with %#v: %s", parmName, index, parm, err)
			if fmt.Sprintf("%s", err) == "bad parameter or other API misuse (Stmt.Bind) (bad parameter or other API misuse)" {
				s, err = d.db.Prepare(stmt.SQL())
				if err == nil {
					*stmt = *s
					err = stmt.BindByIndex(index, parm)
					Goose.Query.Logf(0, "AGAIN: bound %s (%d) with %#v: %s", parmName, index, parm, err)
				}
//			} else {
//				Goose.Query.Logf(0, "[%s]!=[bad parameter or other API misuse (Stmt.Bind) (bad parameter or other API misuse)]", err)
			}
			if err != nil {
//				Goose.Query.Logf(1, "Error binding on list of table %s for %s: %s", tabName, parmName, err)
//				Goose.Query.Logf(1, "SQL: %s", stmt.SQL())
				return err
			}
		} else {
			Goose.Query.Logf(1, "bind error %s with %#v on table %s => %s", parmName, parm, tabName, err)
		}
	}

	return nil
}
