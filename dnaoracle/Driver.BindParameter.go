package dnaoracle

import (
//	"fmt"
	"time"
	"math/big"
	"database/sql/driver"
   "github.com/sijms/go-ora/v2"
   "github.com/luisfurquim/dna"
)

func (drv *Driver) BindParameter(tabName string, at dna.At) ([]driver.NamedValue, error) {
	var parmName string
	var parm interface{}
	var err error
	var vals []driver.NamedValue
	var v string
	var num *go_ora.Number

	vals = make([]driver.NamedValue, 0, len(at.By))

	for parmName, parm = range at.By {
		switch p := parm.(type) {
		case []byte:
			vals = append(vals, driver.NamedValue{
				Name: parmName,
				Value: go_ora.Blob{
					Data: p,
				},
			})


		case string:
			if len(p) < longString {
				vals = append(vals, driver.NamedValue{
					Name: parmName,
					Value: go_ora.NVarChar(p),
				})
			} else {
				vals = append(vals, driver.NamedValue{
					Name: parmName,
					Value: go_ora.NClob{
						String: p,
						Valid: true,
					},
				})
			}

		case nil, float64:
			vals = append(vals, driver.NamedValue{
				Name: parmName,
				Value: p,
			})

		case time.Time:
			vals = append(vals, driver.NamedValue{
				Name: parmName,
				Value: go_ora.TimeStampTZ(p),
			})

		case bool:
			if p {
				v = "T"
			} else {
				v = "F"
			}
			vals = append(vals, driver.NamedValue{
				Name: parmName,
				Value: v,
			})

		case big.Int:
			num, err = go_ora.NewNumberFromString(p.String())
			if err != nil {
				Goose.Query.Logf(1, "bind error %s with %#v on table %s: %s", parmName, parm, tabName, err)
				return nil, err
			}
			vals = append(vals, driver.NamedValue{
				Name: parmName,
				Value: num,
			})

		case big.Float:
			num, err = go_ora.NewNumberFromString(p.String())
			if err != nil {
				Goose.Query.Logf(1, "bind error %s with %#v on table %s: %s", parmName, parm, tabName, err)
				return nil, err
			}
			vals = append(vals, driver.NamedValue{
				Name: parmName,
				Value: num,
			})

		case int8, int16, int32, int64, int, uint8, uint16, uint32, uint64, uint, float32, dna.PK:
			num, err = go_ora.NewNumber(p)
			if err != nil {
				Goose.Query.Logf(1, "bind error %s with %#v on table %s: %s", parmName, parm, tabName, err)
				return nil, err
			}
			vals = append(vals, driver.NamedValue{
				Name: parmName,
				Value: num,
			})

		default:
			Goose.Query.Logf(1, "bind unsupported type for: %T -> %#v", p, p)
		}
	}

	return vals, nil
}
