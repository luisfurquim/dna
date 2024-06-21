package dna

func fieldJoinNameVal(fld []FieldSpec) string {
	var f FieldSpec
	var s string

	for _, f = range fld {
		if f.JoinList {
			continue
		}
		if len(s)>0 {
			s += ","
		}
		s += "`" + f.Name + "`=?"
	}

	return s
}

