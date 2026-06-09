package dnaoracle

import (
	"fmt"
	"reflect"
	"strconv"

	"github.com/luisfurquim/dna"
)

// oracleTypeFromFieldSpec returns the Oracle SQL column type for a FieldSpec.
// The use_quotes parameter is not used here (column type doesn't need quoting).
func oracleTypeFromFieldSpec(f dna.FieldSpec, use_quotes bool) (string, error) {
	var prec, frac int

	if f.PK {
		if len(f.Prec) >= 1 {
			fmt.Sscanf(f.Prec[0], "%d", &prec)
		} else {
			prec = 38
		}
		return fmt.Sprintf("NUMBER(%d)", prec), nil
	}

	if len(f.Fk) > 0 {
		return "NUMBER", nil
	}

	switch f.Type.Kind() {
	case reflect.Bool:
		return "CHAR(1 BYTE)", nil

	case reflect.Int8, reflect.Uint8:
		prec = 3
		if len(f.Prec) >= 1 {
			fmt.Sscanf(f.Prec[0], "%d", &prec)
			if prec > 3 {
				prec = 3
			}
		}
		return fmt.Sprintf("NUMBER(%d)", prec), nil

	case reflect.Int16, reflect.Uint16:
		prec = 5
		if len(f.Prec) >= 1 {
			fmt.Sscanf(f.Prec[0], "%d", &prec)
			if prec > 5 {
				prec = 5
			}
		}
		return fmt.Sprintf("NUMBER(%d)", prec), nil

	case reflect.Int32, reflect.Uint32:
		prec = 10
		if len(f.Prec) >= 1 {
			fmt.Sscanf(f.Prec[0], "%d", &prec)
			if prec > 10 {
				prec = 10
			}
		}
		return fmt.Sprintf("NUMBER(%d)", prec), nil

	case reflect.Int, reflect.Uint:
		if strconv.IntSize == 32 {
			prec = 10
		} else {
			prec = 19
		}
		if len(f.Prec) >= 1 {
			fmt.Sscanf(f.Prec[0], "%d", &prec)
			maxPrec := 10
			if strconv.IntSize != 32 {
				maxPrec = 19
			}
			if prec > maxPrec {
				prec = maxPrec
			}
		}
		return fmt.Sprintf("NUMBER(%d)", prec), nil

	case reflect.Int64, reflect.Uint64:
		prec = 19
		if len(f.Prec) >= 1 {
			fmt.Sscanf(f.Prec[0], "%d", &prec)
			if prec > 19 {
				prec = 19
			}
		}
		return fmt.Sprintf("NUMBER(%d)", prec), nil

	case reflect.Float32:
		return "BINARY_FLOAT", nil

	case reflect.Float64:
		return "BINARY_DOUBLE", nil

	case reflect.Struct:
		if f.Type == BigInt {
			prec = 38
			if len(f.Prec) >= 1 {
				fmt.Sscanf(f.Prec[0], "%d", &prec)
				if prec > 38 {
					prec = 38
				}
			}
			return fmt.Sprintf("NUMBER(%d)", prec), nil
		}
		if f.Type == BigFloat {
			prec = 38
			frac = 34
			if len(f.Prec) >= 1 {
				fmt.Sscanf(f.Prec[0], "%d", &prec)
				if prec > 38 {
					prec = 38
				}
			}
			if len(f.Prec) >= 2 {
				fmt.Sscanf(f.Prec[1], "%d", &frac)
				if frac > 34 {
					frac = 34
				}
			}
			return fmt.Sprintf("NUMBER(%d,%d)", prec, frac), nil
		}
		if f.Type == Time {
			prec = 9
			if len(f.Prec) >= 1 {
				fmt.Sscanf(f.Prec[0], "%d", &prec)
				if prec > 9 {
					prec = 9
				}
			}
			return fmt.Sprintf("TIMESTAMP (%d) WITH TIME ZONE", prec), nil
		}
		return "", ErrUnsupportedType

	case reflect.Array, reflect.Slice:
		return "BLOB", nil

	case reflect.String:
		if len(f.Prec) >= 1 {
			fmt.Sscanf(f.Prec[0], "%d", &prec)
		} else {
			return "", ErrSyntax
		}
		if prec < longString {
			return fmt.Sprintf("NVARCHAR2(%d)", prec), nil
		}
		return "NCLOB", nil

	default:
		return "", ErrUnsupportedType
	}
}
