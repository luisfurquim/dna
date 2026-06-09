package dnaoci

import (
	"database/sql"
	"math/big"
	"strings"
	"time"

	"github.com/luisfurquim/dna"
)

// bindArgs converts the dna.At.By map into a slice of interface{} suitable
// for database/sql named parameters (via sql.Named).
// Only parameters referenced in sqlStr (as :name) are included.
func (drv *Driver) bindArgs(tabName string, at dna.At, sqlStr string) ([]interface{}, error) {
	args := make([]interface{}, 0, len(at.By))

	for parmName, parm := range at.By {
		if !strings.Contains(sqlStr, ":"+parmName) {
			continue
		}

		switch p := parm.(type) {
		case []byte:
			args = append(args, sql.Named(parmName, p))

		case string:
			args = append(args, sql.Named(parmName, p))

		case nil:
			args = append(args, sql.Named(parmName, nil))

		case float64, float32:
			args = append(args, sql.Named(parmName, p))

		case time.Time:
			args = append(args, sql.Named(parmName, p))

		case bool:
			var v string
			if p {
				v = "T"
			} else {
				v = "F"
			}
			args = append(args, sql.Named(parmName, v))

		case big.Int:
			args = append(args, sql.Named(parmName, p.String()))

		case big.Float:
			args = append(args, sql.Named(parmName, p.String()))

		case int8, int16, int32, int64, int, uint8, uint16, uint32, uint64, uint:
			args = append(args, sql.Named(parmName, p))

		case dna.PK:
			args = append(args, sql.Named(parmName, int64(p)))

		default:
			Goose.Query.Logf(1, "bind unsupported type for: %T -> %#v", p, p)
			args = append(args, sql.Named(parmName, p))
		}
	}

	return args, nil
}
