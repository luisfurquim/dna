package dna

import (
	"errors"
	"fmt"
	"strings"
)

// VersionRecord stores the schema version for a single table in the __VERSION__ table.
type VersionRecord struct {
	TableName   string
	VersionHash string
	Definition  string // canonical JSON of []FieldSpec
	UpdatedAt   string // ISO 8601 timestamp
}

// ColumnChange describes how a single column changed between schema versions.
type ColumnChange struct {
	OldField    FieldSpec
	NewField    FieldSpec
	MigrateExpr string // neutral expression from `migrate:` tag (empty if auto-convertible)
}

// TableDiff describes the full set of changes between old and new schema for one table.
type TableDiff struct {
	TableName string
	Added     []FieldSpec       // columns in new but not old
	Removed   []FieldSpec       // columns in old but not new
	Changed   []ColumnChange    // columns present in both but different
	Renamed   map[string]string // old_name -> new_name
	OldFields []FieldSpec       // complete old field list
	NewFields []FieldSpec       // complete new field list
}

// TableMigration describes the migration plan for one table (used in MigrationPlan).
type TableMigration struct {
	TableName     string
	Status        MigrationStatus
	Diff          *TableDiff
	Warnings      []string
	AutoConverted []ColumnChange
	RequiresData  bool
}

// MigrationStatus indicates what will happen to a table during migration.
type MigrationStatus byte

const (
	MigrationUnchanged MigrationStatus = iota
	MigrationCreated
	MigrationModified
)

// MigrationPlan is returned by ExplainMigration for dry-run inspection.
type MigrationPlan struct {
	Tables   []TableMigration
	Warnings []string
}

// String produces a human-readable migration report.
func (mp *MigrationPlan) String() string {
	var sb strings.Builder

	sb.WriteString("Migration Plan:\n")

	if len(mp.Warnings) > 0 {
		sb.WriteString("\nGlobal Warnings:\n")
		for _, w := range mp.Warnings {
			fmt.Fprintf(&sb, "  WARNING: %s\n", w)
		}
	}

	for _, tm := range mp.Tables {
		fmt.Fprintf(&sb, "\n  Table %q:\n", tm.TableName)

		switch tm.Status {
		case MigrationUnchanged:
			sb.WriteString("    STATUS: Unchanged\n")
			continue
		case MigrationCreated:
			sb.WriteString("    STATUS: Created (new table)\n")
			continue
		case MigrationModified:
			sb.WriteString("    STATUS: Modified\n")
		}

		if tm.Diff == nil {
			continue
		}

		for _, f := range tm.Diff.Added {
			fmt.Fprintf(&sb, "    + ADD COLUMN %s (%s)\n", f.Name, typeString(f.Type))
		}

		for _, c := range tm.Diff.Changed {
			if c.MigrateExpr != "" {
				fmt.Fprintf(&sb, "    ~ CHANGE %s: %s -> %s [migrate: %s]\n",
					c.NewField.Name, typeString(c.OldField.Type), typeString(c.NewField.Type), c.MigrateExpr)
			} else {
				fmt.Fprintf(&sb, "    ~ CHANGE %s: %s -> %s (auto)\n",
					c.NewField.Name, typeString(c.OldField.Type), typeString(c.NewField.Type))
			}
		}

		for oldName, newName := range tm.Diff.Renamed {
			fmt.Fprintf(&sb, "    > RENAME %s -> %s\n", oldName, newName)
		}

		for _, f := range tm.Diff.Removed {
			fmt.Fprintf(&sb, "    - DROP COLUMN %s\n", f.Name)
		}

		for _, w := range tm.Warnings {
			fmt.Fprintf(&sb, "    WARNING: %s\n", w)
		}
	}

	return sb.String()
}

var ErrNarrowingConversion error = errors.New("Narrowing conversion requires explicit migrate tag")
var ErrMigrationFailed     error = errors.New("Migration failed")
var ErrDriverMismatch      error = errors.New("Source and target drivers must be same concrete type")
var ErrMigrateFromNotFound error = errors.New("migrate_from references non-existent old column")
var ErrNotMigrationDriver  error = errors.New("Driver does not implement MigrationDriver")
