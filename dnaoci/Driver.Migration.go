package dnaoci

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/luisfurquim/dna"
)

// CreateVersionTable creates the __VERSION__ table if it does not exist.
func (drv *Driver) CreateVersionTable() error {
	sqlStr := `CREATE TABLE "__VERSION__" ("table_name" VARCHAR2(200) PRIMARY KEY, "version_hash" VARCHAR2(64) NOT NULL, "definition" CLOB NOT NULL, "updated_at" VARCHAR2(30) NOT NULL)`

	_, err := drv.db.Exec(sqlStr)
	if err != nil {
		errStr := fmt.Sprintf("%s", err)
		if strings.HasPrefix(errStr, "ORA-00955") || strings.HasPrefix(errStr, "ORA-02264") {
			return nil
		}
		return err
	}
	return nil
}

// GetVersion retrieves the version record for a table.
func (drv *Driver) GetVersion(tableName string) (*dna.VersionRecord, error) {
	sqlStr := `SELECT "version_hash", "definition", "updated_at" FROM "__VERSION__" WHERE "table_name" = :1`

	var hash, def, updatedAt string
	err := drv.db.QueryRow(sqlStr, tableName).Scan(&hash, &def, &updatedAt)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}

	return &dna.VersionRecord{
		TableName:   tableName,
		VersionHash: hash,
		Definition:  def,
		UpdatedAt:   updatedAt,
	}, nil
}

// SetVersion inserts or updates the version record for a table.
func (drv *Driver) SetVersion(rec dna.VersionRecord) error {
	sqlStr := `MERGE INTO "__VERSION__" v
		USING (SELECT :1 AS "table_name" FROM DUAL) s
		ON (v."table_name" = s."table_name")
		WHEN MATCHED THEN UPDATE SET
			"version_hash" = :2,
			"definition" = :3,
			"updated_at" = :4
		WHEN NOT MATCHED THEN INSERT
			("table_name", "version_hash", "definition", "updated_at")
			VALUES (:5, :6, :7, :8)`

	_, err := drv.db.Exec(sqlStr,
		rec.TableName, rec.VersionHash, rec.Definition, rec.UpdatedAt,
		rec.TableName, rec.VersionHash, rec.Definition, rec.UpdatedAt)
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

	// Build column list
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
	if err := target.CreateTable(tableName, fields); err != nil {
		return fmt.Errorf("CopyTableTo: create target table %s: %w", tableName, err)
	}

	// Read all rows from source
	selectSQL := fmt.Sprintf("SELECT %s FROM %s%s%s", cols, qt, tableName, qt)
	rows, err := drv.db.Query(selectSQL)
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
	for rows.Next() {
		dest := make([]interface{}, nCols)
		ptrs := make([]interface{}, nCols)
		for i := range dest {
			ptrs[i] = &dest[i]
		}

		if err := rows.Scan(ptrs...); err != nil {
			return fmt.Errorf("CopyTableTo: scan row: %w", err)
		}

		if _, err := target.db.Exec(insertSQL, dest...); err != nil {
			return fmt.Errorf("CopyTableTo: insert row: %w", err)
		}
	}

	return rows.Err()
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
		colType, err := oracleTypeFromFieldSpec(f)
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
			tmpCol := "__mig_" + colName
			colType, err := oracleTypeFromFieldSpec(change.NewField)
			if err != nil {
				return fmt.Errorf("MigrateTable: change column %s: %w", colName, err)
			}

			sqlStr := fmt.Sprintf("ALTER TABLE %s%s%s ADD (%s%s%s %s)",
				qt, tabName, qt, qt, tmpCol, qt, colType)
			Goose.Init.Logf(4, "Migration SQL: %s", sqlStr)
			if _, err := drv.db.Exec(sqlStr); err != nil {
				return fmt.Errorf("MigrateTable: add temp column %s: %w", tmpCol, err)
			}

			sqlStr = fmt.Sprintf("UPDATE %s%s%s SET %s%s%s = %s",
				qt, tabName, qt, qt, tmpCol, qt, migrateExpr)
			Goose.Init.Logf(4, "Migration SQL: %s", sqlStr)
			if _, err := drv.db.Exec(sqlStr); err != nil {
				return fmt.Errorf("MigrateTable: update column %s: %w", colName, err)
			}

			sqlStr = fmt.Sprintf("ALTER TABLE %s%s%s DROP COLUMN %s%s%s",
				qt, tabName, qt, qt, colName, qt)
			Goose.Init.Logf(4, "Migration SQL: %s", sqlStr)
			if _, err := drv.db.Exec(sqlStr); err != nil {
				return fmt.Errorf("MigrateTable: drop old column %s: %w", colName, err)
			}

			sqlStr = fmt.Sprintf("ALTER TABLE %s%s%s RENAME COLUMN %s%s%s TO %s%s%s",
				qt, tabName, qt, qt, tmpCol, qt, qt, colName, qt)
			Goose.Init.Logf(4, "Migration SQL: %s", sqlStr)
			if _, err := drv.db.Exec(sqlStr); err != nil {
				return fmt.Errorf("MigrateTable: rename temp column %s to %s: %w", tmpCol, colName, err)
			}
		} else {
			colType, err := oracleTypeFromFieldSpec(change.NewField)
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
