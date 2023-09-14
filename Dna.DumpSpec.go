package dna

import (
	"fmt"
)

func (d *Dna) DumpSpec(tabName string) string {
	var list *list
	var ruleName string
	var join int
	var rule tabRule
	var dump string
	var ok bool

	if _, ok = d.list[tabName]; ok {
		for ruleName, list = range d.list[tabName] {
			dump += "Rule name: " + ruleName + "\n"
			for join, rule = range list.joins {
				dump += fmt.Sprintf("%d: %#v\n", join, rule)
			}
			dump += "\n"
		}
	}

	return dump
}



