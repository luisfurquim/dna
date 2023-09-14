package dna

import (
	"fmt"
)

func (d *Dna) GetSpec(tabName, ruleName string, join int) string {
	var ok bool

	if _, ok = d.list[tabName]; ok {
		if _, ok = d.list[tabName][ruleName]; ok {
			if _, ok = d.list[tabName][ruleName].joins[join]; ok {
				return d.list[tabName][ruleName].joins[join].rule
			}
		}
	}

	return ""
}



