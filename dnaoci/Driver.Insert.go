package dnaoci

import (
	"database/sql"
	"database/sql/driver"

	"github.com/luisfurquim/dna"
)

func oraValue(v interface{}) interface{} {
	switch p := v.(type) {
	case bool:
		if p {
			return "T"
		}
		return "F"
	case dna.PK:
		return int64(p)
	default:
		return v
	}
}

func (drv *Driver) Insert(tabName string, pk driver.NamedValue, parms []driver.NamedValue) (dna.PK, error) {
	var err error
	var entry *stmtEntry
	var ok bool
	var id int64

	if _, ok = drv.insert[tabName]; !ok {
		Goose.Query.Logf(1, "Error: no insert statements for table %s: %s", tabName, ErrNoStmtForTable)
		return 0, ErrNoStmtForTable
	}

	if entry, ok = drv.insert[tabName]["*"]; !ok {
		Goose.Query.Logf(1, "Error: no insert statement for table %s: %s", tabName, ErrNoStmtForRule)
		return 0, ErrNoStmtForRule
	}

	// Build named args for godror (uses sql.Named)
	args := make([]interface{}, 0, len(parms)+1)
	for _, p := range parms {
		if p.Name != "" {
			args = append(args, sql.Named(p.Name, oraValue(p.Value)))
		} else {
			args = append(args, oraValue(p.Value))
		}
	}

	// godror supports RETURNING ... INTO :name via sql.Named with sql.Out
	args = append(args, sql.Named("DNA_LAST_INSERTED", sql.Out{Dest: &id}))

	Goose.Query.Logf(5, "tabName: %s", tabName)
	Goose.Query.Logf(5, "SQL: %s", entry.sql)
	Goose.Query.Logf(6, "Parms: %#v", args)

	_, err = entry.stmt.Exec(args...)
	if err != nil {
		Goose.Query.Logf(1, "Error executing insert on table %s: %s, SQL: %s", tabName, err, entry.sql)
		return 0, err
	}

	Goose.Query.Logf(4, "LastInsertId on table %s: %d", tabName, id)
	return dna.PK(id), nil
}
