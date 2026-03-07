package dnasqlite

import (
	"database/sql/driver"
	"strings"
	"github.com/luisfurquim/dna"
)

func (drv *Driver) Insert(tabName string, pk driver.NamedValue, parms []driver.NamedValue) (dna.PK, error) {
	var id int64
	var i int
	var err error
	var plist []interface{}
	var p driver.NamedValue
	var isIndexed bool

	plist = make([]interface{}, len(parms))

	if len(parms)>1 {
		if parms[1].Ordinal != parms[0].Ordinal {
			isIndexed = true
		}
	}

	if isIndexed {
		for _, p = range parms {
			plist[p.Ordinal] = p.Value
		}
	} else {
		for i, p = range parms {
			plist[i] = p.Value
		}
	}

	id, err = drv.insert[tabName]["*"].Insert(plist...)

	if err != nil {
		Goose.Query.Logf(1, "Error: %s, plist: %#v, SQL: %s", err, plist, drv.insert[tabName]["*"].SQL())
		
		// Workaround for corrupted prepared statement: attempt to recompile and retry once
		if strings.Contains(err.Error(), "bad parameter or other API misuse") {
			Goose.Query.Logf(1, "Detected potential prepared statement corruption for table %s, attempting to recompile and retry", tabName)
			
			// Get the current statement's SQL for reconstruction
			currentSQL := drv.insert[tabName]["*"].SQL()
			
			// Close the corrupted statement
			drv.insert[tabName]["*"].Finalize()
			
			// Recompile the statement
			newStmt, recompileErr := drv.db.Prepare(currentSQL)
			if recompileErr != nil {
				Goose.Query.Logf(1, "Failed to recompile prepared statement for table %s: %s", tabName, recompileErr)
				return dna.PK(id), err // Return original error
			}
			
			// Replace the corrupted statement
			drv.insert[tabName]["*"] = newStmt
			
			// Retry the insert operation
			id, err = drv.insert[tabName]["*"].Insert(plist...)
			if err != nil {
				Goose.Query.Logf(1, "Retry after recompile failed for table %s: %s", tabName, err)
			} else {
				Goose.Query.Logf(1, "Successfully recovered from prepared statement corruption for table %s", tabName)
			}
		}
	}
	
	return dna.PK(id), err
}

	
