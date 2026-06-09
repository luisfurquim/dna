package dnaoci

import (
	"github.com/luisfurquim/dna"
)

func (drv *Driver) Delete(tabName string, at dna.At) error {
	var err error
	var entry *stmtEntry
	var ok bool
	var args []interface{}

	if _, ok = drv.delete[tabName]; !ok {
		Goose.Query.Logf(1, "Error: no delete statements for table %s: %s", tabName, ErrNoStmtForTable)
		return ErrNoStmtForTable
	}

	if entry, ok = drv.delete[tabName][at.With]; !ok {
		Goose.Query.Logf(1, "Error: no delete statement for table %s, rule %s: %s", tabName, at.With, ErrNoStmtForRule)
		return ErrNoStmtForRule
	}

	args, err = drv.bindArgs(tabName, at, entry.sql)
	if err != nil {
		Goose.Query.Logf(1, "Error binding parameters for table %s, rule %s: %s", tabName, at.With, err)
		return err
	}

	_, err = entry.stmt.Exec(args...)
	if err != nil {
		Goose.Query.Logf(1, "Error deleting from table %s, rule %s, sql: %s: %s", tabName, at.With, entry.sql, err)
	}

	return err
}
