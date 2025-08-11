package dnasqlite

import (
	"database/sql/driver"
)


func (drv *Driver) Update(tabName string, pk driver.NamedValue, parms []driver.NamedValue) error {
	var plist []interface{}
	var i int
	var p driver.NamedValue
	var isIndexed bool

	plist = make([]interface{}, len(parms) + 1)

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

	plist[len(parms)] = pk.Value

	return drv.update[tabName]["id"].Exec(plist...)
}
