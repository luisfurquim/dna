package dnaoracle

import (
	"time"
	"errors"
	"reflect"
	"math/big"
	"database/sql/driver"
	"github.com/sijms/go-ora/v2"
	"github.com/luisfurquim/goose"
)

const longString int = 2000

type Stmt struct {
	go_ora.Stmt
	SQL string
}

type Driver struct {
	db *go_ora.Connection
	insert map[string]map[string]*Stmt
	update map[string]map[string]*Stmt
	find map[string]map[string]*Stmt
	delete map[string]map[string]*Stmt
}

type fnSpec struct{
	Func func(ident string, args []string) (string, []string, error)
	Ident string
	MinArgs int
	MaxArgs int
}

type Scanner struct{
	rows driver.Rows
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