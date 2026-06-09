package dnaoci

import (
	"database/sql"
)

func New(db *sql.DB, use_quotes bool) *Driver {
	return &Driver{
		db:         db,
		use_quotes: use_quotes,
	}
}
