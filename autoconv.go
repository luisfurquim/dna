package dna

import (
	"reflect"
	"strconv"
)

// wideningRules defines which type conversions are safe (no data loss).
// Key is source Kind, value is set of target Kinds that are safe widenings.
var wideningRules = map[reflect.Kind]map[reflect.Kind]bool{
	reflect.Int8: {
		reflect.Int16: true, reflect.Int32: true, reflect.Int64: true, reflect.Int: true,
		reflect.Float32: true, reflect.Float64: true,
	},
	reflect.Int16: {
		reflect.Int32: true, reflect.Int64: true, reflect.Int: true,
		reflect.Float32: true, reflect.Float64: true,
	},
	reflect.Int32: {
		reflect.Int64: true, reflect.Int: true,
		reflect.Float64: true,
	},
	reflect.Int64: {},
	reflect.Int:   {},

	reflect.Uint8: {
		reflect.Uint16: true, reflect.Uint32: true, reflect.Uint64: true, reflect.Uint: true,
		reflect.Int16: true, reflect.Int32: true, reflect.Int64: true, reflect.Int: true,
		reflect.Float32: true, reflect.Float64: true,
	},
	reflect.Uint16: {
		reflect.Uint32: true, reflect.Uint64: true, reflect.Uint: true,
		reflect.Int32: true, reflect.Int64: true, reflect.Int: true,
		reflect.Float32: true, reflect.Float64: true,
	},
	reflect.Uint32: {
		reflect.Uint64: true, reflect.Uint: true,
		reflect.Int64: true,
		reflect.Float64: true,
	},
	reflect.Uint64: {},
	reflect.Uint:   {},

	reflect.Float32: {
		reflect.Float64: true,
	},
	reflect.Float64: {},
}

// IsAutoConvertible returns true if the conversion from oldField to newField
// is a safe widening that can be done automatically without a migrate: tag.
func IsAutoConvertible(oldField, newField FieldSpec) bool {
	if oldField.Type == nil || newField.Type == nil {
		return false
	}

	oldKind := oldField.Type.Kind()
	newKind := newField.Type.Kind()

	// Same kind but different precision (e.g., string prec 50 -> string prec 100)
	if oldKind == newKind {
		return isPrecisionWidening(oldField.Prec, newField.Prec)
	}

	// Check the widening rules table
	if targets, ok := wideningRules[oldKind]; ok {
		return targets[newKind]
	}

	return false
}

// isPrecisionWidening returns true if newPrec is >= oldPrec (widening).
// Returns true if precisions are empty (no constraint change).
func isPrecisionWidening(oldPrec, newPrec []string) bool {
	if len(oldPrec) == 0 && len(newPrec) == 0 {
		return true
	}

	if len(oldPrec) == 0 || len(newPrec) == 0 {
		// One has precision, the other doesn't — this is a change worth noting
		// but if new has no precision (unconstrained), it's effectively wider
		return len(newPrec) == 0
	}

	oldVal, err1 := strconv.Atoi(oldPrec[0])
	newVal, err2 := strconv.Atoi(newPrec[0])
	if err1 != nil || err2 != nil {
		return false
	}

	return newVal >= oldVal
}

// AutoConvExpr returns the neutral expression for automatic type conversion.
// For most safe widenings, the database handles the cast implicitly,
// so this returns the column name itself (identity conversion).
func AutoConvExpr(oldField, newField FieldSpec) string {
	// For safe widenings, the database can handle implicit conversion.
	// The column name is used as-is in the migration SELECT.
	return oldField.Name
}
