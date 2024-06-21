package dna

func fld2stmt(fldList []FieldSpec) ([]StmtColSpec) {
	var f FieldSpec
	var s []StmtColSpec

	for _, f = range fldList {
		if f.JoinList {
			continue
		}
		s = append(s, StmtColSpec{Column: f.Name})
	}

	return s
}
