package dnaoci

import (
	"database/sql"
	"io"

	"github.com/luisfurquim/dna"
)

func (drv *Driver) Select(tabName string, at dna.At, fn func(dna.Scanner) error) error {
	var err error
	var entry *stmtEntry
	var ok bool
	var rows *sql.Rows
	var args []interface{}

	if _, ok = drv.find[tabName]; !ok {
		Goose.Query.Logf(1, "Error: no statements for table %s: %s", tabName, ErrNoStmtForTable)
		return ErrNoStmtForTable
	}

	if entry, ok = drv.find[tabName][at.With]; !ok {
		Goose.Query.Logf(1, "Error: no statement for table %s, rule %s: %s", tabName, at.With, ErrNoStmtForRule)
		return ErrNoStmtForRule
	}

	args, err = drv.bindArgs(tabName, at, entry.sql)
	if err != nil {
		Goose.Query.Logf(1, "Error binding parameters for table %s, rule %s: %s", tabName, at.With, err)
		return err
	}

	rows, err = entry.stmt.Query(args...)
	if err != nil {
		Goose.Query.Logf(1, "Error selecting from table %s, rule %s: %s", tabName, at.With, err)
		return err
	}
	defer rows.Close()

	s := &Scanner{rows: rows}

	for {
		err = fn(s)
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
	}
}
