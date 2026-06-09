package dnaoci

import (
	"fmt"

	"github.com/luisfurquim/dna"
)

func (drv *Driver) Count(tabName string, at dna.At) (int64, error) {
	var err error
	var entry *stmtEntry
	var ok bool
	var args []interface{}
	var count int64

	if _, ok = drv.count[tabName]; !ok {
		Goose.Query.Logf(1, "Error: no statements for table %s: %s", tabName, ErrNoStmtForTable)
		return 0, ErrNoStmtForTable
	}

	if entry, ok = drv.count[tabName][at.With]; !ok {
		Goose.Query.Logf(1, "Error: no statement for table %s, rule %s: %s", tabName, at.With, ErrNoStmtForRule)
		return 0, ErrNoStmtForRule
	}

	args, err = drv.bindArgs(tabName, at, entry.sql)
	if err != nil {
		Goose.Query.Logf(1, "Error binding parameters for table %s, rule %s: %s", tabName, at.With, err)
		return 0, err
	}

	row := entry.stmt.QueryRow(args...)
	err = row.Scan(&count)
	if err != nil {
		Goose.Query.Logf(1, "Error counting on table %s, rule %s: %s", tabName, at.With, err)
		return 0, fmt.Errorf("count %s/%s: %w", tabName, at.With, err)
	}

	return count, nil
}
