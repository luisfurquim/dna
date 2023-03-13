package dna

func (d *Dna) Close() error {
	return d.db.Close()
}

