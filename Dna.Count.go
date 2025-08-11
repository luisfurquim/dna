package dna

/*
import (
   "github.com/gwenn/gosqlite"
)

func (d *Dna) getCount(tabName, rule string) (stmt *sqlite.Stmt, err error) {
	var ok bool
	var r map[string]*sqlite.Stmt

	if r, ok = d.count[tabName]; !ok {
		Goose.Query.Logf(1, "Error counting table %s: %s", tabName, ErrNoTablesFound)
		err = ErrNoTablesFound
		return
	}

	if stmt, ok = r[rule]; !ok {
		Goose.Query.Logf(1, "Error counting table %s: %s", tabName, ErrRuleNotFound)
		Goose.Query.Logf(1, "rule %s: rules %#v", rule, d.count)
		err = ErrRuleNotFound
		return
	}

	return
}
*/

func (d *Dna) Count(at At) (int64, error) {
	var count int64
	var tabName string
	var err error
	var isChan bool

	tabName, _, isChan, err = d.getMultiRefs(at.Table)
	if err != nil {
		Goose.Query.Logf(1, "Parameter type error: %s", ErrNoTablesFound)
		return 0, ErrNoTablesFound
	}

	if isChan {
		return 0, ErrChanNotAllowed
	}

	count, err = d.driver.Count(tabName, at)
	if err != nil {
		Goose.Query.Logf(1, "Count error on %s: %s", tabName, err)
		return 0, err
	}

	return count, nil
}
