package dna

func fieldByName(nm string, fldList []FieldSpec) (int, bool) {
	var i int

	for i=0; i<len(fldList); i++ {
		if nm == fldList[i].Name {
			return fldList[i].Index, true
		}
	}

	return -1, false
}
