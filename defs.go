package dna

import (
	"errors"
	"reflect"
   "github.com/gwenn/gosqlite"
   "github.com/luisfurquim/goose"
)

type SaveOption byte

const(
	NoCascade SaveOption = iota
)

type TableName struct{}
type PK int64
type Find struct{}
type Count struct{}
type Delete struct{}
type Index struct{}
type Save struct{}

type At struct {
	Table interface{}
	With string
	By map[string]interface{}
}

type Schema struct {
	Tables []interface{}
}

type table struct {
	name string
	fields []field
	xrefs map[string]string
	pkName string
	pkIndex int
}

type field struct {
	name string
	fk string
	joinList bool
	index int
}

type tabRule struct {
	table string
	rule string
	targetName string
	targetIndex int
	joinedIndex int
}

type list struct {
	cols []int
	joins map[int]tabRule
	stmt *sqlite.Stmt
}

type listSpec struct {
	cols           []string
	colTypes map[int]string
	sort           []string
	filter           string
	limit				  string
}

type Dna struct {
	tables map[string]table
	tableType map[string]string

	db *sqlite.Conn

	list map[string]map[string]*list

	insert map[string]*sqlite.Stmt
	link map[string]*sqlite.Stmt
	unlink map[string]*sqlite.Stmt
	updateBy map[string]map[string]*sqlite.Stmt
	count map[string]map[string]*sqlite.Stmt
	listJoin map[string]map[string]*sqlite.Stmt
	listBy map[string]map[string]*sqlite.Stmt
	exists map[string]map[string]*sqlite.Stmt
	delete map[string]map[string]*sqlite.Stmt

}

type GooseG struct {
	Init goose.Alert
	Query goose.Alert
}

var Goose GooseG = GooseG{
	Init: goose.Alert(2),
	Query: goose.Alert(2),
}

var ErrSpecNotStruct         error = errors.New("Specification is not of struct type")
var ErrChanNotAllowed        error = errors.New("Channel not allowed")
var ErrNoPKFound             error = errors.New("No primary key found")
var ErrNoTablesFound         error = errors.New("No tables found")
var ErrColumnNotFound        error = errors.New("Column not found")
var ErrRuleNotFound	        error = errors.New("Rule not found")
var ErrNotStructPointer      error = errors.New("Parameter must be of pointer to struct type")
var ErrNotStructSlicePointer error = errors.New("Parameter must be of pointer to slice of pointers to struct type")
var ErrNotStructPointerChan  error = errors.New("Parameter must be of channel of pointer to struct type")
var ErrNoRuleFound           error = errors.New("No rule found")
var ErrInvalid               error = errors.New("Invalid")
var ErrNullColumn            error = errors.New("Null column")
var ErrPKNotI64              error = errors.New("Primary key is not int64")
var ErrWrongParmCount        error = errors.New("Wrong parameter count")


var TableNameType reflect.Type = reflect.TypeOf(TableName{})
var PKType reflect.Type = reflect.TypeOf(PK(0))
var FindType reflect.Type = reflect.TypeOf(Find{})
var CountType reflect.Type = reflect.TypeOf(Count{})
var DeleteType reflect.Type = reflect.TypeOf(Delete{})
var IndexType reflect.Type = reflect.TypeOf(Index{})
var SaveType reflect.Type = reflect.TypeOf(Save{})
