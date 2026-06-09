package dnaoci

import (
	"database/sql"
	"database/sql/driver"
)

func (drv *Driver) Update(tabName string, pk driver.NamedValue, parms []driver.NamedValue) error {
	var err error
	var entry *stmtEntry
	var ok bool

	if _, ok = drv.update[tabName]; !ok {
		Goose.Query.Logf(1, "Error: no update statements for table %s: %s", tabName, ErrNoStmtForTable)
		return ErrNoStmtForTable
	}

	if entry, ok = drv.update[tabName]["id"]; !ok {
		Goose.Query.Logf(1, "Error: no update statement for table %s: %s", tabName, ErrNoStmtForRule)
		return ErrNoStmtForRule
	}

	args := make([]interface{}, 0, len(parms)+1)
	for _, p := range parms {
		if p.Name != "" {
			args = append(args, sql.Named(p.Name, p.Value))
		} else {
			args = append(args, p.Value)
		}
	}

	// PK as last parameter (WHERE clause)
	if pk.Name != "" {
		args = append(args, sql.Named(pk.Name, pk.Value))
	} else {
		args = append(args, pk.Value)
	}

	_, err = entry.stmt.Exec(args...)
	if err != nil {
		Goose.Query.Logf(1, "Error executing update on table %s: %s", tabName, err)
		return err
	}

	return nil
}
