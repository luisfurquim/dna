package dnasqlite

import (
	"github.com/luisfurquim/dna"
)


func (drv *Driver) ColumnSpecs(fldList []dna.FieldSpec, pkIndex int) (string, []int) {
	var f dna.FieldSpec
	var s string
	var i []int

	s = "rowid"
	i = []int{pkIndex}

	for _, f = range fldList {
		if f.JoinList {
			continue
		}
		s += ",`" + f.Name + "`"
		i = append(i, f.Index)
	}

	return s, i
}
