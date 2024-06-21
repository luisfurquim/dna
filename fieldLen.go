package dna

func fieldLen(fld []FieldSpec) int {
	var f FieldSpec
	var n int

	for _, f = range fld {
		if f.JoinList {
			continue
		}
		n++
	}

	return n
}

