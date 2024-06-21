package dna

func (d *Dna) Del(at At) error {
	var tabName string
	var err error

	tabName, _, _, err = d.getMultiRefs(at.Table)
	if err != nil {
		Goose.Query.Logf(1, "Parameter type error: %s", ErrNoTablesFound)
		return ErrNoTablesFound
	}

	err = d.driver.Delete(tabName, at)
	if err != nil {
		Goose.Query.Logf(1, "Delete error on %s: %s", tabName, err)
		return err
	}

	return nil
}