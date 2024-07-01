package dnaoracle

import (
	"github.com/sijms/go-ora/v2"
)

func New(db *go_ora.Connection, use_quotes bool) *Driver {
	return &Driver{
		db: db,
		use_quotes: use_quotes,
	}
}

