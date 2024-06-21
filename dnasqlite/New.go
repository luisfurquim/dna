package dnasqlite

import (
	"github.com/gwenn/gosqlite"
)

func New(db *sqlite.Conn) *Driver {
	return &Driver{db:db}
}

