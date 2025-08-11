package dnasqlite

import (
	"database/sql/driver"
	"github.com/luisfurquim/dna"
)

func (drv *Driver) Insert(tabName string, pk driver.NamedValue, parms []driver.NamedValue) (dna.PK, error) {
	var id int64
	var i int
	var err error
	var plist []interface{}
	var p driver.NamedValue
	var isIndexed bool

	plist = make([]interface{}, len(parms))

	if len(parms)>1 {
		if parms[1].Ordinal != parms[0].Ordinal {
			isIndexed = true
		}
	}

	if isIndexed {
		for _, p = range parms {
			plist[p.Ordinal] = p.Value
		}
	} else {
		for i, p = range parms {
			plist[i] = p.Value
		}
	}

	id, err = drv.insert[tabName]["*"].Insert(plist...)

	if err != nil {
		Goose.Query.Logf(1, "Error: %s, plist: %#v, SQL: %s", err, plist, drv.insert[tabName]["*"].SQL())
	}
	
	return dna.PK(id), err
}

	
