package dna

import (
	"fmt"
	"reflect"
)

// calculateTableDiff computes the differences between old and new schema for a table.
// renameMap maps new column names to old column names (from migrate_from: tags).
func calculateTableDiff(tableName string, oldFields, newFields []FieldSpec, renameMap map[string]string) TableDiff {
	diff := TableDiff{
		TableName: tableName,
		Renamed:   make(map[string]string),
		OldFields: oldFields,
		NewFields: newFields,
	}

	// Build lookup maps
	oldByName := make(map[string]FieldSpec, len(oldFields))
	for _, f := range oldFields {
		oldByName[f.Name] = f
	}

	newByName := make(map[string]FieldSpec, len(newFields))
	for _, f := range newFields {
		newByName[f.Name] = f
	}

	// Track which old columns are accounted for (by rename or match)
	oldAccountedFor := make(map[string]bool, len(oldFields))

	// Process renames first: renameMap is newName -> oldName
	for newName, oldName := range renameMap {
		oldField, oldExists := oldByName[oldName]
		newField, newExists := newByName[newName]

		if !oldExists || !newExists {
			// migrate_from references non-existent column — will be caught later
			continue
		}

		diff.Renamed[oldName] = newName
		oldAccountedFor[oldName] = true

		// Check if the type also changed during the rename
		if !fieldSpecEqual(oldField, newField) {
			diff.Changed = append(diff.Changed, ColumnChange{
				OldField: oldField,
				NewField: newField,
			})
		}
	}

	// Identify added and changed columns
	for _, newField := range newFields {
		// Skip if this column was handled as a rename
		if _, isRenamed := renameMap[newField.Name]; isRenamed {
			continue
		}

		oldField, exists := oldByName[newField.Name]
		if !exists {
			diff.Added = append(diff.Added, newField)
		} else {
			oldAccountedFor[newField.Name] = true
			if !fieldSpecEqual(oldField, newField) {
				diff.Changed = append(diff.Changed, ColumnChange{
					OldField: oldField,
					NewField: newField,
				})
			}
		}
	}

	// Identify removed columns (in old but not accounted for)
	for _, oldField := range oldFields {
		if oldAccountedFor[oldField.Name] {
			continue
		}
		if _, existsInNew := newByName[oldField.Name]; !existsInNew {
			diff.Removed = append(diff.Removed, oldField)
		}
	}

	return diff
}

// fieldSpecEqual compares two FieldSpecs for schema equivalence.
// Ignores Index (which is the struct field index, not a DB property).
func fieldSpecEqual(a, b FieldSpec) bool {
	if a.Name != b.Name {
		return false
	}
	if a.PK != b.PK {
		return false
	}
	if a.Fk != b.Fk {
		return false
	}
	if a.Auto != b.Auto {
		return false
	}
	if !reflect.DeepEqual(a.Prec, b.Prec) {
		// Treat nil and empty slice as equal
		if len(a.Prec) != 0 || len(b.Prec) != 0 {
			return false
		}
	}
	if typeString(a.Type) != typeString(b.Type) {
		return false
	}
	return true
}

// classifyChanges analyzes the changes in a TableDiff and produces warnings.
// Returns an error if any column change requires a migrate: expression but none is provided.
func classifyChanges(diff *TableDiff, migrateExprs map[string]string) (warnings []string, err error) {
	for i := range diff.Changed {
		change := &diff.Changed[i]
		colName := change.NewField.Name

		// Check if there's an explicit migrate expression
		if expr, ok := migrateExprs[colName]; ok {
			change.MigrateExpr = expr
			warnings = append(warnings, fmt.Sprintf(
				"column %q: type change %s -> %s [migrate: %s]",
				colName, typeString(change.OldField.Type), typeString(change.NewField.Type), expr))
			continue
		}

		// Check if auto-convertible
		if IsAutoConvertible(change.OldField, change.NewField) {
			change.MigrateExpr = AutoConvExpr(change.OldField, change.NewField)
			warnings = append(warnings, fmt.Sprintf(
				"column %q: auto-widening %s -> %s",
				colName, typeString(change.OldField.Type), typeString(change.NewField.Type)))
			continue
		}

		// Check if only precision changed (not type)
		if typeString(change.OldField.Type) == typeString(change.NewField.Type) {
			if isPrecisionWidening(change.OldField.Prec, change.NewField.Prec) {
				change.MigrateExpr = change.OldField.Name
				warnings = append(warnings, fmt.Sprintf(
					"column %q: precision widening %v -> %v",
					colName, change.OldField.Prec, change.NewField.Prec))
				continue
			}
			// Precision narrowing
			warnings = append(warnings, fmt.Sprintf(
				"column %q: precision narrowing %v -> %v (possible truncation)",
				colName, change.OldField.Prec, change.NewField.Prec))
			return warnings, fmt.Errorf("%w: column %q changes precision from %v to %v without migrate tag",
				ErrNarrowingConversion, colName, change.OldField.Prec, change.NewField.Prec)
		}

		// Incompatible type change without migrate expression
		return warnings, fmt.Errorf("%w: column %q changes from %s to %s without migrate tag",
			ErrNarrowingConversion, colName, typeString(change.OldField.Type), typeString(change.NewField.Type))
	}

	// Warn about removed columns
	for _, f := range diff.Removed {
		warnings = append(warnings, fmt.Sprintf("column %q will be dropped", f.Name))
	}

	return warnings, nil
}
