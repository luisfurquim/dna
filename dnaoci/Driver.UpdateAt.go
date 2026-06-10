package dnaoci

import (
	"database/sql"
	"database/sql/driver"

	"github.com/luisfurquim/dna"
)

func (drv *Driver) UpdateAt(tabName string, at dna.At, parms []driver.NamedValue) error {
	var err error
	var entry *stmtEntry
	var ok bool

	if _, ok = drv.update[tabName]; !ok {
		Goose.Query.Logf(1, "Error: no update statements for table %s: %s", tabName, ErrNoStmtForTable)
		return ErrNoStmtForTable
	}

	if entry, ok = drv.update[tabName][at.With]; !ok {
		Goose.Query.Logf(1, "Error: no update statement for table %s, rule %s: %s", tabName, at.With, ErrNoStmtForRule)
		return ErrNoStmtForRule
	}

	args := make([]interface{}, 0, len(parms))
	for _, p := range parms {
		if p.Name != "" {
			args = append(args, sql.Named(p.Name, oraValue(p.Value)))
		} else {
			args = append(args, oraValue(p.Value))
		}
	}

	_, err = entry.stmt.Exec(args...)
	if err != nil {
		Goose.Query.Logf(1, "Error executing update on table %s, rule %s: %s", tabName, at.With, err)
		return err
	}

	return nil
}
