package dnaoracle

import (
	"database/sql/driver"
	"fmt"
	"io"
	"strings"

	go_ora "github.com/sijms/go-ora/v2"
	"github.com/luisfurquim/dna"
)

// CreateVersionTable creates the __VERSION__ table if it does not exist.
func (drv *Driver) CreateVersionTable() error {
	var qt string
	if drv.use_quotes {
		qt = `"`
	}

	sql := fmt.Sprintf(`CREATE TABLE %s__VERSION__%s (
		%stable_name%s VARCHAR2(200) PRIMARY KEY,
		%sversion_hash%s VARCHAR2(64) NOT NULL,
		%sdefinition%s CLOB NOT NULL,
		%supdated_at%s VARCHAR2(30) NOT NULL
	)`, qt, qt, qt, qt, qt, qt, qt, qt, qt, qt)

	_, err := drv.db.Exec(sql)
	if err != nil {
		// Ignore "table already exists" errors
		errStr := fmt.Sprintf("%s", err)
		if strings.HasPrefix(errStr, "ORA-00955") || strings.HasPrefix(errStr, "ORA-02264") {
			return nil
		}
		return err
	}
	return nil
}

// GetVersion retrieves the version record for a table.
// Returns nil, nil if no record is found.
func (drv *Driver) GetVersion(tableName string) (*dna.VersionRecord, error) {
	var qt string
	if drv.use_quotes {
		qt = `"`
	}

	sqlStr := fmt.Sprintf("SELECT %sversion_hash%s, %sdefinition%s, %supdated_at%s FROM %s__VERSION__%s WHERE %stable_name%s = :1",
		qt, qt, qt, qt, qt, qt, qt, qt, qt, qt)

	stmt := go_ora.NewStmt(sqlStr, drv.db)
	defer stmt.Close()

	rows, err := stmt.Query([]driver.Value{tableName})
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	dest := make([]driver.Value, 3)
	if err := rows.Next(dest); err != nil {
		if err == io.EOF {
			return nil, nil
		}
		return nil, nil
	}

	hash := fmt.Sprintf("%v", dest[0])
	def := fmt.Sprintf("%v", dest[1])
	updatedAt := fmt.Sprintf("%v", dest[2])

	return &dna.VersionRecord{
		TableName:   tableName,
		VersionHash: hash,
		Definition:  def,
		UpdatedAt:   updatedAt,
	}, nil
}

// SetVersion inserts or updates the version record for a table.
func (drv *Driver) SetVersion(rec dna.VersionRecord) error {
	var qt string
	if drv.use_quotes {
		qt = `"`
	}

	sqlStr := fmt.Sprintf(`MERGE INTO %s__VERSION__%s v
		USING (SELECT :1 AS %stable_name%s FROM DUAL) s
		ON (v.%stable_name%s = s.%stable_name%s)
		WHEN MATCHED THEN UPDATE SET
			%sversion_hash%s = :2,
			%sdefinition%s = :3,
			%supdated_at%s = :4
		WHEN NOT MATCHED THEN INSERT
			(%stable_name%s, %sversion_hash%s, %sdefinition%s, %supdated_at%s)
			VALUES (:5, :6, :7, :8)`,
		qt, qt, qt, qt, qt, qt, qt, qt, qt, qt, qt, qt, qt, qt, qt, qt,
		qt, qt, qt, qt, qt, qt)

	_, err := drv.db.Exec(sqlStr,
		driver.Value(rec.TableName), driver.Value(rec.VersionHash), driver.Value(rec.Definition), driver.Value(rec.UpdatedAt),
		driver.Value(rec.TableName), driver.Value(rec.VersionHash), driver.Value(rec.Definition), driver.Value(rec.UpdatedAt))
	return err
}

// TranslateExpr translates a neutral Go-like expression to Oracle SQL.
func (drv *Driver) TranslateExpr(neutralExpr string, pkName string) (string, error) {
	return expr(neutralExpr)
}

// CopyTableTo copies all data from a table into the same-named table on
// another driver instance. The target must be a *Driver (same concrete type).
func (drv *Driver) CopyTableTo(targetDriver interface{}, tableName string, fields []dna.FieldSpec) error {
	target, ok := targetDriver.(*Driver)
	if !ok {
		return dna.ErrDriverMismatch
	}

	var qt string
	if drv.use_quotes {
		qt = `"`
	}

	// Build column list (excluding JoinList and PK fields for insert)
	var colList []string
	for _, f := range fields {
		if f.JoinList {
			continue
		}
		colList = append(colList, qt+f.Name+qt)
	}
	cols := strings.Join(colList, ",")
	nCols := len(colList)

	// Create table on target
	err := target.CreateTable(tableName, fields)
	if err != nil {
		return fmt.Errorf("CopyTableTo: create target table %s: %w", tableName, err)
	}

	// Read all rows from source
	selectSQL := fmt.Sprintf("SELECT %s FROM %s%s%s", cols, qt, tableName, qt)

	srcStmt := go_ora.NewStmt(selectSQL, drv.db)
	defer srcStmt.Close()

	rows, err := srcStmt.Query(nil)
	if err != nil {
		return fmt.Errorf("CopyTableTo: query: %w", err)
	}
	defer rows.Close()

	// Build insert statement for target
	var tqt string
	if target.use_quotes {
		tqt = `"`
	}
	var tColList []string
	for _, f := range fields {
		if f.JoinList {
			continue
		}
		tColList = append(tColList, tqt+f.Name+tqt)
	}
	tCols := strings.Join(tColList, ",")

	placeholders := make([]string, nCols)
	for i := range placeholders {
		placeholders[i] = fmt.Sprintf(":%d", i+1)
	}
	insertSQL := fmt.Sprintf("INSERT INTO %s%s%s (%s) VALUES (%s)",
		tqt, tableName, tqt, tCols, strings.Join(placeholders, ","))

	// Copy row by row
	for {
		dest := make([]driver.Value, nCols)
		if err := rows.Next(dest); err != nil {
			if err == io.EOF {
				break
			}
			break
		}

		_, err = target.db.Exec(insertSQL, dest...)
		if err != nil {
			return fmt.Errorf("CopyTableTo: insert row: %w", err)
		}
	}

	return nil
}

// MigrateTable applies a TableDiff to the Oracle database.
func (drv *Driver) MigrateTable(diff dna.TableDiff, migrateExprs map[string]string) error {
	var qt string
	if drv.use_quotes {
		qt = `"`
	}

	tabName := diff.TableName

	// Process renames first
	for oldName, newName := range diff.Renamed {
		sqlStr := fmt.Sprintf("ALTER TABLE %s%s%s RENAME COLUMN %s%s%s TO %s%s%s",
			qt, tabName, qt, qt, oldName, qt, qt, newName, qt)
		Goose.Init.Logf(4, "Migration SQL: %s", sqlStr)
		if _, err := drv.db.Exec(sqlStr); err != nil {
			return fmt.Errorf("MigrateTable: rename column %s to %s: %w", oldName, newName, err)
		}
	}

	// Process additions
	for _, f := range diff.Added {
		colType, err := drv.oracleColumnType(f)
		if err != nil {
			return fmt.Errorf("MigrateTable: add column %s: %w", f.Name, err)
		}
		sqlStr := fmt.Sprintf("ALTER TABLE %s%s%s ADD (%s%s%s %s)",
			qt, tabName, qt, qt, f.Name, qt, colType)
		Goose.Init.Logf(4, "Migration SQL: %s", sqlStr)
		if _, err := drv.db.Exec(sqlStr); err != nil {
			return fmt.Errorf("MigrateTable: add column %s: %w", f.Name, err)
		}
	}

	// Process type changes
	for _, change := range diff.Changed {
		colName := change.NewField.Name

		if migrateExpr, ok := migrateExprs[colName]; ok {
			// Complex change: add temp column, update with expression, drop old, rename
			tmpCol := "__mig_" + colName
			colType, err := drv.oracleColumnType(change.NewField)
			if err != nil {
				return fmt.Errorf("MigrateTable: change column %s: %w", colName, err)
			}

			// Add temp column
			sqlStr := fmt.Sprintf("ALTER TABLE %s%s%s ADD (%s%s%s %s)",
				qt, tabName, qt, qt, tmpCol, qt, colType)
			Goose.Init.Logf(4, "Migration SQL: %s", sqlStr)
			if _, err := drv.db.Exec(sqlStr); err != nil {
				return fmt.Errorf("MigrateTable: add temp column %s: %w", tmpCol, err)
			}

			// Update with conversion expression
			sqlStr = fmt.Sprintf("UPDATE %s%s%s SET %s%s%s = %s",
				qt, tabName, qt, qt, tmpCol, qt, migrateExpr)
			Goose.Init.Logf(4, "Migration SQL: %s", sqlStr)
			if _, err := drv.db.Exec(sqlStr); err != nil {
				return fmt.Errorf("MigrateTable: update column %s: %w", colName, err)
			}

			// Drop old column
			sqlStr = fmt.Sprintf("ALTER TABLE %s%s%s DROP COLUMN %s%s%s",
				qt, tabName, qt, qt, colName, qt)
			Goose.Init.Logf(4, "Migration SQL: %s", sqlStr)
			if _, err := drv.db.Exec(sqlStr); err != nil {
				return fmt.Errorf("MigrateTable: drop old column %s: %w", colName, err)
			}

			// Rename temp to final
			sqlStr = fmt.Sprintf("ALTER TABLE %s%s%s RENAME COLUMN %s%s%s TO %s%s%s",
				qt, tabName, qt, qt, tmpCol, qt, qt, colName, qt)
			Goose.Init.Logf(4, "Migration SQL: %s", sqlStr)
			if _, err := drv.db.Exec(sqlStr); err != nil {
				return fmt.Errorf("MigrateTable: rename temp column %s to %s: %w", tmpCol, colName, err)
			}
		} else {
			// Simple type change — try ALTER TABLE MODIFY
			colType, err := drv.oracleColumnType(change.NewField)
			if err != nil {
				return fmt.Errorf("MigrateTable: modify column %s: %w", colName, err)
			}
			sqlStr := fmt.Sprintf("ALTER TABLE %s%s%s MODIFY (%s%s%s %s)",
				qt, tabName, qt, qt, colName, qt, colType)
			Goose.Init.Logf(4, "Migration SQL: %s", sqlStr)
			if _, err := drv.db.Exec(sqlStr); err != nil {
				return fmt.Errorf("MigrateTable: modify column %s: %w", colName, err)
			}
		}
	}

	// Process removals
	for _, f := range diff.Removed {
		sqlStr := fmt.Sprintf("ALTER TABLE %s%s%s DROP COLUMN %s%s%s",
			qt, tabName, qt, qt, f.Name, qt)
		Goose.Init.Logf(4, "Migration SQL: %s", sqlStr)
		if _, err := drv.db.Exec(sqlStr); err != nil {
			return fmt.Errorf("MigrateTable: drop column %s: %w", f.Name, err)
		}
	}

	// Clear cached prepared statements
	delete(drv.find, tabName)
	delete(drv.insert, tabName)
	delete(drv.update, tabName)
	delete(drv.delete, tabName)

	return nil
}

// oracleColumnType returns the Oracle SQL type string for a FieldSpec.
func (drv *Driver) oracleColumnType(f dna.FieldSpec) (string, error) {
	return oracleTypeFromFieldSpec(f, drv.use_quotes)
}
