package dna

import (
	"fmt"
	"reflect"
	"time"
)

// ExplainMigration performs a dry-run migration.
// It takes the same schema as New(), plus a source driver (the current production DB)
// and a target driver (a scratch DB for testing the migration).
// Both drivers must be the same concrete type and implement MigrationDriver.
//
// The function:
//  1. Copies data from source to target for tables that will be migrated
//  2. Applies the migration on the target
//  3. Returns a MigrationPlan describing what would happen
//
// After calling ExplainMigration, the target DB contains the migrated data
// and can be inspected by the developer to verify data integrity.
func ExplainMigration(source Driver, target Driver, schema Schema) (*MigrationPlan, error) {
	// Validate drivers are same concrete type
	if reflect.TypeOf(source) != reflect.TypeOf(target) {
		return nil, ErrDriverMismatch
	}

	// Both must implement MigrationDriver
	srcMig, ok := source.(MigrationDriver)
	if !ok {
		return nil, fmt.Errorf("source: %w", ErrNotMigrationDriver)
	}
	tgtMig, ok := target.(MigrationDriver)
	if !ok {
		return nil, fmt.Errorf("target: %w", ErrNotMigrationDriver)
	}

	if len(schema.Tables) == 0 {
		return nil, ErrNoTablesFound
	}

	// Build table type map (first pass)
	tableTypeMap, err := buildTableTypeMap(schema.Tables)
	if err != nil {
		return nil, err
	}

	// Ensure version tables exist
	if err := srcMig.CreateVersionTable(); err != nil {
		return nil, fmt.Errorf("source CreateVersionTable: %w", err)
	}
	if err := tgtMig.CreateVersionTable(); err != nil {
		return nil, fmt.Errorf("target CreateVersionTable: %w", err)
	}

	plan := &MigrationPlan{}

	// Process each table
	for _, tab := range schema.Tables {
		parsed, err := parseTableSchema(tab, tableTypeMap)
		if err != nil {
			return nil, fmt.Errorf("parse table: %w", err)
		}

		if len(parsed.FldList) == 0 {
			continue
		}

		if parsed.PkIndex < 0 {
			return nil, fmt.Errorf("table %s: %w", parsed.TabName, ErrNoPKFound)
		}

		newHash := VersionHash(parsed.FldList)

		// Get current version from source
		versionRec, err := srcMig.GetVersion(parsed.TabName)
		if err != nil {
			return nil, fmt.Errorf("GetVersion %s: %w", parsed.TabName, err)
		}

		if versionRec == nil {
			// New table — no migration needed
			plan.Tables = append(plan.Tables, TableMigration{
				TableName: parsed.TabName,
				Status:    MigrationCreated,
			})

			// Create it on target so the user can see the schema
			if err := target.CreateTable(parsed.TabName, parsed.FldList); err != nil {
				Goose.Init.Logf(2, "ExplainMigration: error creating new table %s on target: %s", parsed.TabName, err)
			}
			if err := tgtMig.SetVersion(VersionRecord{
				TableName:   parsed.TabName,
				VersionHash: newHash,
				Definition:  CanonicalJSON(parsed.FldList),
				UpdatedAt:   time.Now().UTC().Format(time.RFC3339),
			}); err != nil {
				Goose.Init.Logf(2, "ExplainMigration: error setting version on target for %s: %s", parsed.TabName, err)
			}
			continue
		}

		if versionRec.VersionHash == newHash {
			// Schema unchanged
			plan.Tables = append(plan.Tables, TableMigration{
				TableName: parsed.TabName,
				Status:    MigrationUnchanged,
			})
			continue
		}

		// Schema changed — calculate diff
		oldFields, derr := deserializeFieldSpecs(versionRec.Definition)
		if derr != nil {
			return nil, fmt.Errorf("deserialize old schema for %s: %w", parsed.TabName, derr)
		}

		diff := calculateTableDiff(parsed.TabName, oldFields, parsed.FldList, parsed.RenameMap)

		// Translate migrate expressions
		driverExprs := map[string]string{}
		for colName, neutralExpr := range parsed.MigrateExprs {
			translated, terr := srcMig.TranslateExpr(neutralExpr, parsed.PkColumnName)
			if terr != nil {
				return nil, fmt.Errorf("table %s, column %s: translate migrate expr: %w", parsed.TabName, colName, terr)
			}
			driverExprs[colName] = translated
		}

		// Classify changes and collect warnings
		warnings, cerr := classifyChanges(&diff, parsed.MigrateExprs)
		if cerr != nil {
			plan.Warnings = append(plan.Warnings,
				fmt.Sprintf("table %s: %s", parsed.TabName, cerr))
			// Don't abort — still report what we found
		}

		tm := TableMigration{
			TableName:    parsed.TabName,
			Status:       MigrationModified,
			Diff:         &diff,
			Warnings:     warnings,
			RequiresData: len(diff.Changed) > 0 || len(diff.Renamed) > 0,
		}

		// Collect auto-converted columns
		for _, c := range diff.Changed {
			if IsAutoConvertible(c.OldField, c.NewField) {
				tm.AutoConverted = append(tm.AutoConverted, c)
			}
		}

		// Copy source data to target and apply migration (dry-run)
		if cerr == nil {
			// Copy data from source to target
			if copyErr := srcMig.CopyTableTo(tgtMig, parsed.TabName, oldFields); copyErr != nil {
				tm.Warnings = append(tm.Warnings,
					fmt.Sprintf("dry-run: error copying data: %s", copyErr))
			} else {
				// Apply migration on target
				if migErr := tgtMig.MigrateTable(diff, driverExprs); migErr != nil {
					tm.Warnings = append(tm.Warnings,
						fmt.Sprintf("dry-run: migration failed: %s", migErr))
				} else {
					// Update version on target
					tgtMig.SetVersion(VersionRecord{
						TableName:   parsed.TabName,
						VersionHash: newHash,
						Definition:  CanonicalJSON(parsed.FldList),
						UpdatedAt:   time.Now().UTC().Format(time.RFC3339),
					})
				}
			}
		}

		plan.Tables = append(plan.Tables, tm)
	}

	return plan, nil
}
