package dnaoracle

import (
	"github.com/sijms/go-ora/v2"
)

func New(db *go_ora.Connection) *Driver {
	return &Driver{db:db}
}

