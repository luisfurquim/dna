package dnaoci

import (
	"database/sql"
	"errors"
	"math/big"
	"reflect"
	"time"

	"github.com/luisfurquim/goose"
)

const longString int = 2000

type Driver struct {
	db         *sql.DB
	use_quotes bool
	insert     map[string]map[string]*stmtEntry
	update     map[string]map[string]*stmtEntry
	find       map[string]map[string]*stmtEntry
	count      map[string]map[string]*stmtEntry
	delete     map[string]map[string]*stmtEntry
}

type stmtEntry struct {
	stmt *sql.Stmt
	sql  string
}

type fnSpec struct {
	Func    func(ident string, args []string) (string, []string, error)
	Ident   string
	MinArgs int
	MaxArgs int
}

type Scanner struct {
	rows *sql.Rows
}

type GooseG struct {
	Init  goose.Alert
	Query goose.Alert
}

var Goose GooseG = GooseG{
	Init:  goose.Alert(2),
	Query: goose.Alert(2),
}

var ErrUnsupportedClause error = errors.New("Unsupported clause")
var ErrUnsupportedType error = errors.New("Unsupported type")
var ErrNoColumns error = errors.New("No columns")
var ErrNoStmtForTable error = errors.New("No prepared statements for this table")
var ErrNoStmtForRule error = errors.New("No prepared statements for this rule")
var ErrSyntax error = errors.New("Syntax error")
var ErrUnknownFunc error = errors.New("Unknown function")

var b big.Int
var BigInt reflect.Type = reflect.TypeOf(b)
var b2 big.Float
var BigFloat reflect.Type = reflect.TypeOf(b2)
var t time.Time
var Time reflect.Type = reflect.TypeOf(t)
