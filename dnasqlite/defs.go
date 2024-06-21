package dnasqlite

import (
	"errors"
	"github.com/gwenn/gosqlite"
	"github.com/luisfurquim/goose"
)

type Driver struct {
	db *sqlite.Conn
	insert map[string]map[string]*sqlite.Stmt
	update map[string]map[string]*sqlite.Stmt
	find map[string]map[string]*sqlite.Stmt
	delete map[string]map[string]*sqlite.Stmt
}

type fnSpec struct{
	Func func(ident string, args []string) (string, []string, error)
	Ident string
	MinArgs int
	MaxArgs int
}

type GooseG struct{
   Init   goose.Alert
   Query  goose.Alert
}

var Goose GooseG = GooseG{
	Init:   goose.Alert(2),
	Query:  goose.Alert(2),
}

//var tmFmt map[string]string
//var functions map[string]fnSpec
var ErrUnsupportedClause error = errors.New("Unsupported clause")
var ErrNoColumns error = errors.New("No columns")
var ErrNoStmtForTable error = errors.New("No prepared statements for this table")
var ErrNoStmtForRule error = errors.New("No prepared statements for this rule")
var ErrSyntax error = errors.New("Syntax error")
var ErrUnknownFunc error = errors.New("Unknown function")
