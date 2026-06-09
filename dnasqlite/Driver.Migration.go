package dnasqlite

import (
	"fmt"
	"strings"

	sqlite "github.com/gwenn/gosqlite"
	"github.com/luisfurquim/dna"
)

// CreateVersionTable creates the __VERSION__ table if it does not exist.
func (drv *Driver) CreateVersionTable() error {
	return drv.db.Exec(`CREATE TABLE IF NOT EXISTS ` + "`__VERSION__`" + ` (
		` + "`table_name`" + ` TEXT PRIMARY KEY,
		` + "`version_hash`" + ` TEXT NOT NULL,
		` + "`definition`" + ` TEXT NOT NULL,
		` + "`updated_at`" + ` TEXT NOT NULL
	)`)
}

// GetVersion retrieves the version record for a table.
// Returns nil, nil if no record is found.
func (drv *Driver) GetVersion(tableName string) (*dna.VersionRecord, error) {
	var rec dna.VersionRecord
	var hash, def, updatedAt string

	found, err := drv.db.SelectByID(
		"SELECT `version_hash`, `definition`, `updated_at` FROM `__VERSION__` WHERE `table_name` = ?",
		tableName,
		&hash, &def, &updatedAt,
	)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, nil
	}

	rec.TableName = tableName
	rec.VersionHash = hash
	rec.Definition = def
	rec.UpdatedAt = updatedAt
	return &rec, nil
}

// SetVersion inserts or updates the version record for a table.
func (drv *Driver) SetVersion(rec dna.VersionRecord) error {
	return drv.db.Exec(
		"INSERT OR REPLACE INTO `__VERSION__` (`table_name`, `version_hash`, `definition`, `updated_at`) VALUES (?, ?, ?, ?)",
		rec.TableName, rec.VersionHash, rec.Definition, rec.UpdatedAt,
	)
}

// TranslateExpr translates a neutral Go-like expression to SQLite SQL.
func (drv *Driver) TranslateExpr(neutralExpr string, pkName string) (string, error) {
	return expr(neutralExpr, pkName)
}

// CopyTableTo copies all data from a table into the same-named table on
// another driver instance. The target must be a *Driver (same concrete type).
func (drv *Driver) CopyTableTo(targetDriver interface{}, tableName string, fields []dna.FieldSpec) error {
	target, ok := targetDriver.(*Driver)
	if !ok {
		return dna.ErrDriverMismatch
	}

	// Build column list (excluding JoinList fields)
	var colList []string
	for _, f := range fields {
		if f.JoinList {
			continue
		}
		colList = append(colList, "`"+f.Name+"`")
	}
	cols := strings.Join(colList, ",")
	nCols := len(colList)

	// Create table on target
	err := target.CreateTable(tableName, fields)
	if err != nil {
		return fmt.Errorf("CopyTableTo: create target table %s: %w", tableName, err)
	}

	// Build insert statement
	placeholders := make([]string, nCols)
	for i := range placeholders {
		placeholders[i] = "?"
	}
	insertSQL := fmt.Sprintf("INSERT INTO `%s` (%s) VALUES (%s)",
		tableName, cols, strings.Join(placeholders, ","))

	selectSQL := fmt.Sprintf("SELECT %s FROM `%s`", cols, tableName)

	return drv.db.Select(selectSQL, func(s *sqlite.Stmt) error {
		// Scan all columns as text and insert into target
		vals := make([]interface{}, nCols)
		for i := 0; i < nCols; i++ {
			txt, isNull := s.ScanText(i)
			if isNull {
				vals[i] = nil
			} else {
				vals[i] = txt
			}
		}
		return target.db.Exec(insertSQL, vals...)
	})
}

// MigrateTable applies a TableDiff to the SQLite database.
func (drv *Driver) MigrateTable(diff dna.TableDiff, migrateExprs map[string]string) error {
	// Determine if we can use simple ALTER TABLE ADD COLUMN
	// (only if there are additions and nothing else)
	simpleAdd := len(diff.Added) > 0 &&
		len(diff.Removed) == 0 &&
		len(diff.Changed) == 0 &&
		len(diff.Renamed) == 0

	if simpleAdd {
		return drv.migrateSimpleAdd(diff)
	}

	return drv.migrateComplex(diff, migrateExprs)
}

// migrateSimpleAdd handles the case where only new columns are added.
func (drv *Driver) migrateSimpleAdd(diff dna.TableDiff) error {
	for _, f := range diff.Added {
		colDef := "`" + f.Name + "`"
		if f.PK {
			colDef += " INTEGER PRIMARY KEY"
		}

		sql := fmt.Sprintf("ALTER TABLE `%s` ADD COLUMN %s", diff.TableName, colDef)
		Goose.Init.Logf(4, "Migration ADD COLUMN: %s", sql)
		if err := drv.db.Exec(sql); err != nil {
			return fmt.Errorf("migrateSimpleAdd: %s: %w", sql, err)
		}
	}
	return nil
}

// migrateComplex handles migrations that require the create-new/copy/drop/rename strategy.
func (drv *Driver) migrateComplex(diff dna.TableDiff, migrateExprs map[string]string) error {
	tabName := diff.TableName
	tmpName := "__mig_" + tabName

	// Build new table column definitions
	var newColDefs []string
	for _, f := range diff.NewFields {
		if f.JoinList {
			continue
		}
		colDef := "`" + f.Name + "`"
		if f.PK {
			colDef += " INTEGER PRIMARY KEY"
		}
		newColDefs = append(newColDefs, colDef)
	}

	// Build the SELECT expressions for data migration
	// Map old column names for lookup
	oldByName := make(map[string]dna.FieldSpec, len(diff.OldFields))
	for _, f := range diff.OldFields {
		oldByName[f.Name] = f
	}

	// Build reverse rename map: newName -> oldName
	reverseRename := make(map[string]string, len(diff.Renamed))
	for oldName, newName := range diff.Renamed {
		reverseRename[newName] = oldName
	}

	// Build the list of target columns and source expressions
	var targetCols []string
	var sourceExprs []string

	for _, newField := range diff.NewFields {
		if newField.JoinList {
			continue
		}

		// Skip PK — SQLite handles rowid automatically
		if newField.PK {
			targetCols = append(targetCols, "`"+newField.Name+"`")
			sourceExprs = append(sourceExprs, "`"+newField.Name+"`")
			continue
		}

		// Check if this column has a migrate expression
		if expr, ok := migrateExprs[newField.Name]; ok {
			targetCols = append(targetCols, "`"+newField.Name+"`")
			sourceExprs = append(sourceExprs, expr)
			continue
		}

		// Check if this is a renamed column
		if oldName, ok := reverseRename[newField.Name]; ok {
			if _, exists := oldByName[oldName]; exists {
				targetCols = append(targetCols, "`"+newField.Name+"`")
				sourceExprs = append(sourceExprs, "`"+oldName+"`")
				continue
			}
		}

		// Check if this column exists in old schema (unchanged or auto-converted)
		if _, exists := oldByName[newField.Name]; exists {
			targetCols = append(targetCols, "`"+newField.Name+"`")
			sourceExprs = append(sourceExprs, "`"+newField.Name+"`")
			continue
		}

		// New column — will get NULL/default, don't include in SELECT
	}

	// Execute migration in a transaction
	Goose.Init.Logf(3, "Migration: complex migration for table %s", tabName)

	// Disable foreign keys during migration
	if err := drv.db.Exec("PRAGMA foreign_keys=OFF"); err != nil {
		return fmt.Errorf("migrateComplex: disable foreign keys: %w", err)
	}

	// Begin transaction
	if err := drv.db.Begin(); err != nil {
		drv.db.Exec("PRAGMA foreign_keys=ON")
		return fmt.Errorf("migrateComplex: begin transaction: %w", err)
	}

	// Create temporary table with new schema
	createSQL := fmt.Sprintf("CREATE TABLE `%s` (%s)", tmpName, strings.Join(newColDefs, ","))
	Goose.Init.Logf(4, "Migration SQL: %s", createSQL)
	if err := drv.db.Exec(createSQL); err != nil {
		drv.db.Rollback()
		drv.db.Exec("PRAGMA foreign_keys=ON")
		return fmt.Errorf("migrateComplex: create temp table: %w", err)
	}

	// Copy data from old table to new
	if len(targetCols) > 0 && len(sourceExprs) > 0 {
		copySQL := fmt.Sprintf("INSERT INTO `%s` (%s) SELECT %s FROM `%s`",
			tmpName,
			strings.Join(targetCols, ","),
			strings.Join(sourceExprs, ","),
			tabName)
		Goose.Init.Logf(4, "Migration SQL: %s", copySQL)
		if err := drv.db.Exec(copySQL); err != nil {
			drv.db.Rollback()
			drv.db.Exec("PRAGMA foreign_keys=ON")
			return fmt.Errorf("migrateComplex: copy data: %w", err)
		}
	}

	// Drop old table
	dropSQL := fmt.Sprintf("DROP TABLE `%s`", tabName)
	Goose.Init.Logf(4, "Migration SQL: %s", dropSQL)
	if err := drv.db.Exec(dropSQL); err != nil {
		drv.db.Rollback()
		drv.db.Exec("PRAGMA foreign_keys=ON")
		return fmt.Errorf("migrateComplex: drop old table: %w", err)
	}

	// Rename temp table to original name
	renameSQL := fmt.Sprintf("ALTER TABLE `%s` RENAME TO `%s`", tmpName, tabName)
	Goose.Init.Logf(4, "Migration SQL: %s", renameSQL)
	if err := drv.db.Exec(renameSQL); err != nil {
		drv.db.Rollback()
		drv.db.Exec("PRAGMA foreign_keys=ON")
		return fmt.Errorf("migrateComplex: rename table: %w", err)
	}

	// Commit transaction
	if err := drv.db.Commit(); err != nil {
		drv.db.Exec("PRAGMA foreign_keys=ON")
		return fmt.Errorf("migrateComplex: commit: %w", err)
	}

	// Re-enable foreign keys
	drv.db.Exec("PRAGMA foreign_keys=ON")

	// Clear cached prepared statements for this table since they're now invalid
	delete(drv.find, tabName)
	delete(drv.insert, tabName)
	delete(drv.update, tabName)
	delete(drv.count, tabName)
	delete(drv.delete, tabName)

	return nil
}
