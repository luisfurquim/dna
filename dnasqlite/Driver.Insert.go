package dnasqlite

import (
	"github.com/luisfurquim/dna"
)

func (drv *Driver) Insert(tabName string, parms ...interface{}) (dna.PK, error) {
	var id int64
	var err error

	id, err = drv.insert[tabName]["*"].Insert(parms...)
	
	return dna.PK(id), err
}

	
