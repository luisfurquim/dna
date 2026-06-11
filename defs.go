package dna

import (
	"errors"
	"reflect"
	"database/sql/driver"
   "github.com/luisfurquim/goose"
)

type SaveOption byte

const(
	NoCascade SaveOption = iota
)

const (
	OptMigrate    uint64 = 1 << iota
	OptSkipCreate
)

type Clause byte
const (
	SelectClause Clause = iota
	InsertClause
	UpdateClause
	DeleteClause
	CountClause
)

type ColType byte
const (
	VarColType ColType = iota
	StringColType
	OtherColType
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
	fields []FieldSpec
	xrefs map[string]string
	pkName string
	pkIndex int
}

type FieldSpec struct {
	Name string
	Fk string
	JoinList bool
	Index int
	Type reflect.Type
	Prec []string
	PK   bool
	Auto bool
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
//	stmt *sqlite.Stmt
}

type saveInfo struct {
	cols []int // indices no array fields da tabela
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
	list map[string]map[string]*list
	saveList map[string]map[string]*saveInfo
	driver Driver
}

type Driver interface {
	// If the database has a special name for PK columns, it must return this name.
	// Otherwise it must return empty string ("").
	PKName() string
	Close() error
	ColumnSpecs(fldList []FieldSpec, pkIndex int) 	(colNames string, cols []int)
	CreateTable(tabName string, columns []FieldSpec) error
	Prepare(stmt *StmtSpec) error
	Select(tabName string, at At, callback func(Scanner) error) error
	Insert(tabName string, pk driver.NamedValue, parms []driver.NamedValue) (PK, error)
	Update(tabName string, pk driver.NamedValue, parms []driver.NamedValue) error
	Delete(tabName string, at At) error
	UpdateAt(tabName string, at At, parms []driver.NamedValue) error
	Count(tabName string, at At) (int64, error)
	Exists(tabName string) bool
}

type Scanner interface{
	Scan(parameters ...interface{}) error
}

// MigrationDriver extends Driver with schema migration capabilities.
// Drivers that implement this interface enable automatic schema migration
// when the struct definition changes between application versions.
// Drivers that do NOT implement this interface continue to work with
// the legacy create-if-not-exists behavior.
type MigrationDriver interface {
	Driver

	// CreateVersionTable creates the __VERSION__ table if it does not exist.
	CreateVersionTable() error

	// GetVersion retrieves the version record for a table.
	// Returns nil, nil if no record is found.
	GetVersion(tableName string) (*VersionRecord, error)

	// SetVersion inserts or updates the version record for a table.
	SetVersion(rec VersionRecord) error

	// MigrateTable applies a TableDiff to the database.
	// The migrateExprs map contains column names to driver-specific SQL expressions
	// for data conversion (already translated from neutral form via TranslateExpr).
	MigrateTable(diff TableDiff, migrateExprs map[string]string) error

	// TranslateExpr translates a neutral Go-like expression (from a migrate: tag)
	// to driver-specific SQL. Uses the same expression system as the by: tags.
	TranslateExpr(neutralExpr string, pkName string) (string, error)

	// CopyTableTo copies all data from a table into the same-named table
	// on another driver instance. Used by ExplainMigration for dry-run.
	// The target must be the same concrete driver type.
	// The targetDriver parameter is interface{} to avoid circular type constraints;
	// each driver implementation asserts it to its own concrete type.
	CopyTableTo(targetDriver interface{}, tableName string, fields []FieldSpec) error
}

type StmtColSpec struct{
	Column  string
	Value   string
	Type    ColType
	Pk      bool
}

type StmtSpec struct{
	Clause           	 Clause
	Table           	 string
	PkName             string
	PkFieldName        string
	Rule				 	 string
	Columns          []StmtColSpec
	Aliases    map[int]string
	ColFunc    map[int]string
	Sort             []string
	SortDir          []string
	Filter             string
	FilterFunc map[int]string
	Group            []string
	GroupFunc  map[int]string
	Limit              string
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
var ErrTooManyOptions        error = errors.New("Too many options")


var TableNameType reflect.Type = reflect.TypeOf(TableName{})
var PKType reflect.Type = reflect.TypeOf(PK(0))
var FindType reflect.Type = reflect.TypeOf(Find{})
var CountType reflect.Type = reflect.TypeOf(Count{})
var DeleteType reflect.Type = reflect.TypeOf(Delete{})
var IndexType reflect.Type = reflect.TypeOf(Index{})
var SaveType reflect.Type = reflect.TypeOf(Save{})
