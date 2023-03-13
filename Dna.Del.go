package dna

func (d *Dna) Del(at At) error {
	var tabName string
	var err error

	tabName, _, _, err = d.getMultiRefs(at.Table)
	if err != nil {
		Goose.Query.Logf(1, "Parameter type error: %s", ErrNoTablesFound)
		return ErrNoTablesFound
	}

	err = d.BindParameter(tabName, at, d.delete[tabName][at.With])
	if err != nil {
		Goose.Query.Logf(1, "Bind parameter error: %s", err)
		return err
	}

	err = d.delete[tabName][at.With].Exec()
	if err != nil {
		Goose.Query.Logf(1, "Delete error on %s: %s", tabName, err)
		return err
	}

	return nil
}