package dna

func (d *Dna) Close() error {
	return d.driver.Close()
}

